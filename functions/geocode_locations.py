
import pandas as pd, numpy as np, matplotlib.pyplot as plt, logging as lg
import time, requests, re, json, os.path, datetime as dt
import spotipy as sp
from geopy.distance import great_circle
#from mpl_toolkits.basemap import Basemap
# create a logger to capture progress
log = lg.getLogger('mb_geocoder')
if not getattr(log, 'handler_set', None):
    todays_date = dt.datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
    log_filename = 'logs/mb_geocoder_{}.log'.format(todays_date)
    #handler = lg.FileHandler(log_filename, encoding='utf-8')
    #formatter = lg.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    #handler.setFormatter(formatter)
    #log.addHandler(handler)
    #log.setLevel(lg.INFO)
    #log.handler_set = True

output_filename = 'data/geocode/mb_geocoded.csv'

# configure local caching
geocode_cache_filename = 'data/geocode/geocode_cache.js'
cache_save_frequency = 10
requests_count = 0
geocode_cache = json.load(open(geocode_cache_filename)) if os.path.isfile(geocode_cache_filename) else {}

# define pause durations
pause_nominatim = 0.95
pause_google = 0.15

# make a http request to api and return the result
def make_request(url):
    log.info('requesting {}'.format(url))
    return requests.get(url).json()


# use nominatim api to geocode an address and return latlng string
def geocode_nominatim(address):
    time.sleep(pause_nominatim)
    url = 'https://nominatim.openstreetmap.org/search?format=json&q={}'
    data = make_request(url.format(address))
    if len(data) > 0:
        return '{},{}'.format(data[0]['lat'], data[0]['lon'])




# use google maps api to geocode an address and return latlng string
def geocode_google(address):
    time.sleep(pause_google)
    url = 'http://maps.googleapis.com/maps/api/geocode/json?sensor=false&address={}'
    data = make_request(url.format(address))
    if len(data['results']) > 0:
        lat = data['results'][0]['geometry']['location']['lat']
        lng = data['results'][0]['geometry']['location']['lng']
        return '{},{}'.format(lat, lng)


# handle geocoding, either from local cache or from one of defined geocoding functions that call APIs
def geocode(address, geocode_function=geocode_nominatim, use_cache=True):

    global geocode_cache, requests_count


    if use_cache and address in geocode_cache and pd.notnull(geocode_cache[address]):
        log.info('retrieving lat-long from cache for place "{}"'.format(address))
        return geocode_cache[address]
    else:
        requests_count += 1
        latlng = geocode_function(address)
        geocode_cache[address] = latlng
        log.info('stored lat-long in cache for place "{}"'.format(address))

        if requests_count % cache_save_frequency == 0:
            save_cache_to_disk(geocode_cache, geocode_cache_filename)

        return latlng

# to improve geocoding accuracy, remove anything in parentheses or square brackets
# example: turn 'Tarlac, Luzon (Region III), Philippines' into 'Tarlac, Luzon, Philippines'
regex = re.compile('\\(.*\\)|\\[.*\\]')
def clean_place_full(place_full):
    if isinstance(place_full, str):
        return regex.sub('', place_full).replace(' ,', ',').replace('  ', ' ')


# parse out the country name in strings with greater geographic detail
def get_country_if_more_detail(address):
    tokens = address.split(',')
    if len(tokens) > 1:
        return tokens[-1].strip()


# save local cache object in memory to disk as JSON
def save_cache_to_disk(cache, filename):
    with open(filename, 'w', encoding='utf-8') as cache_file:
        cache_file.write(json.dumps(cache))
    log.info('saved {:,} cached items to {}'.format(len(cache.keys()), filename))


def get_all_latlons_for_music(all_data, ignore_country_if_more_detail = False):

    # load the dataset
    artists = all_data.copy()
    print('{:,} total artists'.format(len(artists)))

    # clean place_full to remove anything in parentheses or brackets and change empty strings to nulls
    artists['place_full'] = artists['place_full'].map(clean_place_full)
    artists.loc[artists['place_full']=='', 'place_full'] = None

    # drop nulls and get the unique set of places
    addresses = pd.Series(artists['place_full'].dropna().sort_values().unique())
    print('{:,} unique places'.format(len(addresses)))

    # only keep places that are just countries if that country does not exist with city or state elsewhere in list
    if ignore_country_if_more_detail:
        countries_with_more_detail = pd.Series(addresses.map(get_country_if_more_detail).dropna().sort_values().unique())
        print('{:,} countries with more detail'.format(len(countries_with_more_detail)))
        addresses_to_geocode = addresses[~addresses.isin(countries_with_more_detail)]
        print('{:,} unique addresses to geocode'.format(len(addresses_to_geocode)))
    else:
        addresses_to_geocode = addresses

    # geocode (with nominatim) each retained address (ie, full place name string)
    start_time = time.time()
    latlng_dict = {}
    for address in addresses_to_geocode:
        latlng_dict[address] = geocode(address, geocode_function=geocode_nominatim)

    print('geocoded {:,} addresses in {:,.2f} seconds'.format(len(addresses_to_geocode), int(time.time()-start_time)))
    print('received {:,} non-null lat-longs'.format(len([key for key in latlng_dict if latlng_dict[key] is not None])))

    # which addresses failed to geocode successfully?
    addresses_to_geocode = [ key for key in latlng_dict if latlng_dict[key] is None ]
    print('{} addresses still lack lat-long'.format(len(addresses_to_geocode)))


    # now geocode (with google) each address that failed
    start_time = time.time()
    for address in addresses_to_geocode:
        latlng_dict[address] = geocode(address, geocode_function=geocode_google)


    print('geocoded {:,} addresses in {:,.2f} seconds'.format(len(addresses_to_geocode), int(time.time()-start_time)))
    print('received {:,} non-null lat-longs'.format(len([key for key in latlng_dict if latlng_dict[key] is not None])))

    # for each artist, if their place appears in the geocoded dict, pull the latlng value from dict into new df column
    def get_latlng_by_address(address):
        try:
            return latlng_dict[address]
        except:
            return None

    artists['place_latlng'] = artists['place_full'].map(get_latlng_by_address)



    return artists
