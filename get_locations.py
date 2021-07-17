# make a http request to musicbrainz api and return the result
# configure URLs and user-agent header
import pandas as pd, requests, time, json, os.path
import logging as lg, datetime as dt
import requests
import pandas as pd
import time
import pylast
import hashlib
from datetime import date
from datetime import datetime
import requests


pause_standard = 1.1
pause_exceeded_rate = 2

csv_filename = 'place_data/mb.csv'

# create a logger to capture progress
# create a logger to capture progress
log = lg.getLogger('mb')
if not getattr(log, 'handler_set', None):
    todays_date = dt.datetime.today().strftime('%Y_%m_%d_%H_%M')
    log_filename = 'logs/mb_{}.log'.format(todays_date)
    #handler = lg.FileHandler(log_filename, encoding='utf-8')
    #formatter = lg.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    #handler.setFormatter(formatter)
    #log.addHandler(handler)
    #log.setLevel(lg.INFO)
    #log.handler_set = True


# configure URLs and user-agent header
artist_name_url = 'https://musicbrainz.org/ws/2/artist/?query=artist:{}&fmt=json'
artist_id_url = 'https://musicbrainz.org/ws/2/artist/{}?fmt=json'
area_id_url = 'https://musicbrainz.org/ws/2/area/{}?inc=area-rels&fmt=json'
headers = {'User-Agent':'pollystewart'}

# configure local caching
area_cache_filename = 'place_data/area_cache.js'
artist_cache_filename = 'place_data/artist_cache.js'
cache_save_frequency = 10
area_requests_count = 0
artist_requests_count = 0
area_cache = json.load(open(area_cache_filename)) if os.path.isfile(area_cache_filename) else {}
artist_cache = json.load(open(artist_cache_filename)) if os.path.isfile(artist_cache_filename) else {}


def make_request(url, headers= {'User-Agent':'pollystewart'}, attempt_count=1):

    global pause_standard

    time.sleep(pause_standard)
    log.info('request: {}'.format(url))
    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        log.error('requests.get failed: {} {} {}'.format(type(e), e, response.json()))

    if response.status_code == 200: #if status OK
        return {'status_code':response.status_code, 'json':response.json()}

    elif response.status_code == 503: #if status error (server busy or rate limit exceeded)
        try:
            if 'exceeding the allowable rate limit' in response.json()['error']:
                #pause_standard = pause_standard + 0.1
                log.warning('exceeded allowable rate limit, pause_standard is now {} seconds'.format(pause_standard))
                log.warning('details: {}'.format(response.json()))
                time.sleep(pause_exceeded_rate)
        except:
            pass

        next_attempt_count = attempt_count + 1
        log.warning('request failed with status_code 503, so we will try it again with attempt #{}'.format(next_attempt_count))
        return make_request(url, attempt_count=next_attempt_count)

    else: #if other status code, display info and return None for caller to handle
        log.error('make_request failed: status_code {} {}'.format(response.status_code, response.json()))
        return None


# query the musicbrainz api for an artist's name and return the resulting id
def get_artist_id_by_name(name, headers= {'User-Agent':'pollystewart'}):
    response = make_request(artist_name_url.format(name))
    try:
        if response is not None:
            result = response['json']
            artist_id = result['artists'][0]['id']
            return artist_id
    except:
        log.error('get_artist_id_by_name error: {}'.format(response))



# parse the details of an artist from the API response
def extract_artist_details_from_response(response):
    try:
        if response is not None:
            result = response['json']
            artist_details = {'id':result['id'],
                              'name':result['name'],
                              'type':result['type'],
                              'gender':result['gender'],
                              'country':result['country'],
                              'begin_date':None,
                              'end_date':None,
                              'area_id':None,
                              'area_name':None,
                              'begin_area_id':None,
                              'begin_area_name':None,
                              'place_id':None,
                              'place':None}

            if result['life-span'] is not None and 'begin' in result['life-span'] and 'end' in result['life-span']:
                artist_details['begin_date'] = result['life-span']['begin']
                artist_details['end_date'] = result['life-span']['end']
            if result['area'] is not None and 'id' in result['area'] and 'name' in result['area']:
                artist_details['area_id'] = result['area']['id']
                artist_details['area_name'] = result['area']['name']
            if result['begin_area'] is not None and 'id' in result['begin_area'] and 'name' in result['begin_area']:
                artist_details['begin_area_id'] = result['begin_area']['id']
                artist_details['begin_area_name'] = result['begin_area']['name']

            # populate place with begin_area_name if it's not null, else area_name if it's not null, else None
            if artist_details['begin_area_name'] is not None:
                artist_details['place'] = artist_details['begin_area_name']
                artist_details['place_id'] = artist_details['begin_area_id']
            elif artist_details['area_name'] is not None:
                artist_details['place'] = artist_details['area_name']
                artist_details['place_id'] = artist_details['area_id']

            return artist_details

    except:
        log.error('get_artist_by_id error: {}'.format(response))


# get an artist object from the musicbrainz api by the musicbrainz artist id
def get_artist_by_id(artist_id):

    global artist_cache, artist_requests_count

    # first, get the artist details either from the cache or from the API
    if artist_id in artist_cache:
        # if we've looked up this ID before, get it from the cache
        log.info('retrieving artist details from cache for ID {}'.format(artist_id))
        artist_details = artist_cache[artist_id]
    else:
        # if we haven't looked up this ID before, look it up from API now
        response = make_request(artist_id_url.format(artist_id))
        artist_details = extract_artist_details_from_response(response)

        # add this artist to the cache so we don't have to ask the API for it again
        artist_cache[artist_id] = artist_details
        log.info('adding artist details to cache for ID {}'.format(artist_id))

        # save the artist cache to disk once per every cache_save_frequency API requests
        artist_requests_count += 1
        if artist_requests_count % cache_save_frequency == 0: save_cache_to_disk(artist_cache, artist_cache_filename)

    # now that we have the artist details...
    return artist_details


# create a dataframe of artist details and place info from a list of artist IDs
def make_artists_df(artist_ids, row_labels=None, df=None, csv_save_frequency=100):

    # create a list of row labels if caller didn't pass one in
    if row_labels is None:
        row_labels = range(len(artist_ids))

    # create a new dataframe if caller didn't pass an existing one in
    cols = ['id', 'name', 'type', 'gender', 'country', 'begin_date', 'end_date',
            'begin_area_id', 'begin_area_name', 'area_id', 'area_name', 'place_id', 'place']
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(columns=cols)

    start_time = time.time()
    for artist_id, n in zip(artist_ids, row_labels):
        try:
            # get the artist info object
            artist = get_artist_by_id(artist_id)

            # create (or update) a df row containing the data from this artist object
            df.loc[n] = [ artist[col] for col in cols ]
            log.info('successfully got artist details #{:,}: artist_id={}'.format(n, artist_id))

            # save csv dataset to disk once per every csv_save_frequency rows
            if n % csv_save_frequency == 0: df.to_csv(csv_filename, index=False, encoding='utf-8')

        except Exception as e:
            log.error('row #{} failed: {}'.format(n, e))
            pass

    df.to_csv(csv_filename, index=False, encoding='utf-8')
    finish_time = time.time()
    message = 'processed {:,} artists in {:,} seconds and saved csv'.format(len(artist_ids), round(finish_time-start_time, 2))
    log.info(message)
    print(message)

    return df


# parse the details of an area object from the API response
def extract_area_details_from_response(response):
    area_details = {}
    try:
        area_details['name'] = response['json']['name']
        if 'relations' in response['json']:
            for relation in response['json']['relations']:
                if relation['direction']=='backward' and relation['type']=='part of':
                    area_details['parent_id'] = relation['area']['id']
                    area_details['parent_name'] = relation['area']['name']
        else:
            log.warning('area returned no relations: {}'.format(result))
        return area_details
    except Exception as e:
        log.error('extract_area_details_from_response failed: {}'.format(response))
        return None


# get details of an 'area' from the musicbrainz api by area id
def get_area(area_id, full_area_str=''):

    global area_cache, area_requests_count

    # first, get the area details either from the cache or from the API
    if area_id in area_cache:
        # if we've looked up this ID before, get it from the cache
        log.info('retrieving area details from cache for ID {}'.format(area_id))
        area_details = area_cache[area_id]
    else:
        # if we haven't looked up this ID before, look it up from API now
        response = make_request(area_id_url.format(area_id))
        area_details = extract_area_details_from_response(response)

        # add this area to the cache so we don't have to ask the API for it again
        area_cache[area_id] = area_details
        log.info('adding area details to cache for ID {}'.format(area_id))

        # save the area cache to disk once per every cache_save_frequency API requests
        area_requests_count += 1
        if area_requests_count % cache_save_frequency == 0: save_cache_to_disk(area_cache, area_cache_filename)

    # now that we have the area details...
    try:
        if full_area_str == '':
            full_area_str = area_details['name']
        if 'parent_name' in area_details and 'parent_id' in area_details:
            full_area_str = '{}, {}'.format(full_area_str, area_details['parent_name'])
            return area_details['parent_id'], full_area_str #recursively get parent's details
        else:
            # if no parents exist, we're done
            return None, full_area_str
    except Exception as e:
        log.error('get_area error: {}'.format(e))
        return None, full_area_str


# construct a full name from an area ID
# recursively traverse the API, getting coarser-grained place details each time until top-level country
def get_place_full_name_by_area_id(area_id):
    area_name=''
    while area_id is not None:
        area_id, area_name = get_area(area_id, area_name)
    return area_name


# take a list of place IDs and return a dict linking each to its constructed full name
def get_place_full(unique_place_ids):
    start_time = time.time()
    message = 'we will attempt to get place full names for {:,} place IDs'.format(len(unique_place_ids))
    log.info(message)
    print(message)

    place_ids_names = {}
    for place_id, n in zip(unique_place_ids, range(len(unique_place_ids))):
        try:
            place_name = get_place_full_name_by_area_id(place_id)
        except:
            place_name = None
        place_ids_names[place_id] = place_name
        log.info('successfully created place #{:,}: "{}" from place ID "{}"'.format(n + 1, place_name, place_id))

    message = 'finished getting place full names from place IDs in {:.2f} seconds'.format(time.time()-start_time)
    log.info(message)
    print(message)
    return place_ids_names





# save local cache object in memory to disk as JSON
def save_cache_to_disk(cache, filename):
    with open(filename, 'w', encoding='utf-8') as cache_file:
        cache_file.write(json.dumps(cache))
    log.info('saved {:,} cached items to {}'.format(len(cache.keys()), filename))


def get_data_for_finding_locations(df_mbid):
    # load the artist IDs from the lastfm scrobble history data set
    scrobbles = df_mbid.copy()
    artist_ids = scrobbles['artist_mbid'].dropna().unique()#[1000:1005]
    message = 'there are {:,} unique artists to get details for'.format(len(artist_ids))
    log.info(message)
    print(message)
    return scrobbles, artist_ids




def labels_to_retry(df, artist_ids):

    # get all the row labels missing in the df (due to errors that prevented row creation)
    missing_row_labels = [ label for label in range(len(artist_ids)) if label not in df.index ]

    # get the artist mbid for each
    row_labels_to_retry = sorted(missing_row_labels)
    artist_ids_to_retry = [ artist_ids[label] for label in row_labels_to_retry ]

    message = '{} artists to retry'.format(len(artist_ids_to_retry))
    log.info(message)
    print(message)



    return row_labels_to_retry, artist_ids_to_retry

# find place id in dict (created by get_place_full) and return its constructed full name



def find_all_locations(df_mbid):

    scrobbles, artist_ids = get_data_for_finding_locations(df_mbid)
    df = make_artists_df(artist_ids)
    row_labels_to_retry, artist_ids_to_retry = labels_to_retry(df, artist_ids)
    df = make_artists_df(artist_ids_to_retry, row_labels_to_retry, df)
    # create a dict where keys are area IDs and values are full place names from MB API
    unique_place_ids = df['place_id'].dropna().unique()
    place_ids_names = get_place_full(unique_place_ids)

    def get_place_full_from_dict(place_id):
        try:
            return place_ids_names[place_id]
        except:
            return None

    # for each row in dataframe, pull place_full from the place_ids_names dict by place_id
    df['place_full'] = df['place_id'].map(get_place_full_from_dict)

    # for some reason MB constructs Irish places' country as "Ireland, Ireland" - so clean up the duplicate
    df['place_full'] = df['place_full'].str.replace('Ireland, Ireland', 'Ireland')

    df_to_join = df[['id', 'type', 'gender', 'country', 'begin_date', 'end_date', 'area_name', 'place', 'place_full' ]]
    df_to_join.rename(columns={'id':'artist_mbid'}, inplace=True)
    all_data = pd.merge(df_mbid, df_to_join, on = "artist_mbid", how = "left", validate = "many_to_one")




    return all_data
