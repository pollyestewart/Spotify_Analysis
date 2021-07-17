from mutagen.id3 import TCON
import time
import pylast
import hashlib
from datetime import date
from datetime import datetime
import requests
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


# create function to return dataframe containing recently played tracks
def get_all_recent_tracks(user, network, limit_value = 15):
    recent_tracks_list = []
    user = network.get_user(user)
    recent_tracks = user.get_recent_tracks(limit = limit_value, cacheable=True)
    for i, track in enumerate(recent_tracks):
        track_list = []
        track_list.append(track.track.artist.name)
        track_list.append(track.track.title)
        track_list.append(track.album)
        track_list.append(track.timestamp)
        track_list.append(track.track.get_duration())
        track_list.append(track.track.get_listener_count())
        track_list.append(track.track.get_playcount())
        recent_tracks_list.append(track_list)

    df = pd.DataFrame(recent_tracks_list)
    df.columns = ["artist", "track", "album", "timestamp",  "track_duration", "listener_count", "playcount"]
    df['timestamp']  = pd.to_datetime(df['timestamp'] , unit='s')  + pd.Timedelta(hours=1)

    return df


genre_cache = {}
all_genres = TCON.GENRES
def artist_to_genre(artist, network):
        tags = network.get_artist(artist).get_top_tags()
        all_tags = []
        for tag in tags:
            genre_cache[artist] = tag[0].name.title()
            all_tags.append(tag[0].name.title())
        return all_tags

def track_to_genre(artist, track, network):
        tags = network.get_track(artist, track).get_top_tags()
        all_tags = []
        for tag in tags:
            genre_cache[artist, track] = tag[0].name.title()
            all_tags.append(tag[0].name.title())
        return all_tags


def get_all_tags(df, network):

    all_artist_tags = []
    top_3_artist_tags = []

    all_track_tags = []
    top_3_track_tags = []

    for index, rows in df.iterrows():
        # find column containing row info
        artist = rows['artist']
        track = rows['track']

        # pull tags
        artist_tags_list = artist_to_genre(artist, network)
        track_tags_list = track_to_genre(artist, track, network)

        # get a list containing all tags
        all_artist_tags.append(artist_tags_list)
        all_track_tags.append(track_tags_list)

        # get a list containing top 3 tags & append to main list
        top_artist_tags_list = artist_tags_list[:3]
        top_3_artist_tags.append(top_artist_tags_list)

        top_track_tags_list = track_tags_list[:3]
        top_3_track_tags.append(top_track_tags_list)


    df['all_artist_tags'] =  all_artist_tags
    df['top_3_artist_tags'] =  top_3_artist_tags
    df['all_track_tags'] =  all_track_tags
    df['top_3_track_tags'] =  top_3_track_tags

    return df

def get_mbid_of_all_rows(df, network):

    track_publised_date_list = []
    artist_mbid_list = []
    track_mbid_list = []
    album_mbid_list = []

    for index, rows in df.iterrows():

        artist_value = rows['artist']
        track_value = rows['track']
        album_value = rows['album']

        artist = pylast.Artist(artist_value, network)
        track = pylast.Track(title = track_value, network = network, artist = artist_value)
        album = pylast.Album(title = album_value, network = network, artist = artist_value)

        track_published_date = track.get_wiki_published_date()
        artist_mbid = artist.get_mbid()
        track_mbid = track.get_mbid()
        album_mbid = album.get_mbid()


        track_publised_date_list.append(track_published_date)
        artist_mbid_list.append(artist_mbid)
        track_mbid_list.append(track_mbid)
        album_mbid_list.append(album_mbid)

    df['track_published_date'] = track_publised_date_list
    df['artist_mbid'] = artist_mbid_list
    df['track_mbid'] = track_mbid_list
    df['album_mbid'] = album_mbid_list

    return df


def get_spotipy_id(df, sp = spotipy.Spotify(), cid ="3fc91dc129ff4936beb0411d23003200", secret = "baa0c78d715f4497a8baedd3677516db" ):

    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


    df_drop_duplicates = df.drop_duplicates(['artist', 'track'])


    artist_name_list = []
    track_name_list = []
    track_id_list = []
    popularity_list = []


    for index, row in df_drop_duplicates.iterrows():
            artist_name = row['artist']
            track_name = row['track']

            track_results = sp.search(q='artist:' + artist_name + ' track:' + track_name, type='track')
            for i, t in enumerate(track_results['tracks']['items']):
                artist_name_list.append(t['artists'][0]['name'])
                track_name_list.append(t['name'])
                track_id_list.append(t['id'])
                popularity_list.append(t['popularity'])

    df_spotify = pd.DataFrame()

    df_spotify['artist'] = artist_name_list
    df_spotify['track'] = track_name_list
    df_spotify['track_id'] = track_id_list
    df_spotify['popularity'] = popularity_list



    #all_data = pd.DataFrame()
    #all_data['track_id_list'] = track_id_list

    #df['spotipy_track_id'] = track_id_list



    return df_spotify



def get_spotify_data(df, batchsize_value = 100, cid ="3fc91dc129ff4936beb0411d23003200", secret = "baa0c78d715f4497a8baedd3677516db"  ):


    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    df_tracks = df.copy()
    batchsize = batchsize_value
    rows = []
    None_counter = 0
    for i in range(0,len(df_tracks['track_id']),batchsize):
        batch = df_tracks['track_id'][i:i+batchsize]
        feature_results = sp.audio_features(batch)
        for i, t in enumerate(feature_results):
            if t == None:
                None_counter = None_counter + 1
            else:
                rows.append(t)

    df_tracks = pd.DataFrame(rows)
    df_tracks = df_tracks.rename(columns={"id": "track_id"})



    return df_tracks
