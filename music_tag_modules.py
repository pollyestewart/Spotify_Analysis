import pandas as pd
from datetime import date, datetime, timedelta
from datetime import datetime
import datetime
import plotly.express as px
import plotly.graph_objects as go
import numpy as np


def split_out_tags(df, track_or_artist = "track"):

    if track_or_artist == "track":

        df['top_3_track_tags_clean'] = df.top_3_track_tags.replace(r"[][']","", regex=True)

        foo = lambda x: pd.Series([i for i in reversed(x.split(','))])
        rev = df['top_3_track_tags_clean'].apply(foo)
        rev.rename(columns={0:'track_tag_3',1:'track_tag_2',2:'track_tag_1'},inplace=True)
        rev = rev[['track_tag_1','track_tag_2','track_tag_3']]

        df['track_tag_1'] = rev['track_tag_1']
        df['track_tag_2'] = rev['track_tag_2']
        df['track_tag_3'] = rev['track_tag_3']

        df = df.drop(columns = ['top_3_track_tags_clean'])

    elif track_or_artist == "artist":
        df['top_3_artist_tags_clean'] = df.top_3_artist_tags.replace(r"[][']","", regex=True)

        foo = lambda x: pd.Series([i for i in reversed(x.split(','))])
        rev = df['top_3_artist_tags_clean'].apply(foo)
        rev.rename(columns={0:'artist_tag_3',1:'artist_tag_2',2:'artist_tag_1'},inplace=True)
        rev = rev[['artist_tag_1','artist_tag_2','artist_tag_3']]

        df['artist_tag_1'] = rev['artist_tag_1']
        df['artist_tag_2'] = rev['artist_tag_2']
        df['artist_tag_3'] = rev['artist_tag_3']

        df = df.drop(columns = ['top_3_artist_tags_clean'])

    else:
        df = df.copy()

    return df





def time_since_last_played_column(df):

    df['today_date'] = datetime.datetime.today()
    df['today_date'] = pd.to_datetime(df['today_date'], format='%Y-%m-%d %H:%M:%S')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S') - timedelta(hours=1, minutes=0)

    df['minutes_since_last_played'] = (df['today_date'] - df['timestamp']).astype('timedelta64[m]')
    df['days_since_last_played'] = df['minutes_since_last_played'] / 60 / 24

    return df


def most_popular_tags_over_time(df, no_of_tags_to_display = 100):

    df4 = df.fillna("")

    df4['track_tag_1'] = np.where(df4['track_tag_1'] == "", df4['artist_tag_1'], df4['track_tag_1'])
    df4['track_tag_2'] = np.where(df4['track_tag_2'] == "", df4['artist_tag_2'], df4['track_tag_2'])
    df4['track_tag_3'] = np.where(df4['track_tag_3'] == "", df4['artist_tag_3'], df4['track_tag_3'])

    df4 = df4[['timestamp', 'track_tag_1', 'track_tag_2', 'track_tag_3']]

    df4['date'] = pd.to_datetime(df4["timestamp"]).dt.strftime('%Y-%m-%d')
    df4 = df4.drop(columns = 'timestamp')

    df4 = pd.melt(df4, id_vars =['date'])
    df4['tag_count'] = 1
    df4 = df4.drop(columns = 'variable')

    df4['value'] = df4['value'].str.lower()
    df4 = df4.replace(r'[,\"\'] ','', regex=True).replace(r'\s*([^\s]+)\s*', r'\1', regex=True)

    total = df4.groupby(['value'], as_index=False).aggregate({'tag_count': 'sum'}).sort_values('tag_count', ascending = False)
    top_tags = total.head(no_of_tags_to_display)
    top_tags = list(top_tags['value'])
    df4 = df4[df4['value'].isin(top_tags)]
    df4 = df4.groupby(['date', 'value'], as_index=False).aggregate({'tag_count': 'sum'}).sort_values('tag_count', ascending = False)

    df4 = df4.sort_values("date")

    df4.columns = df4.columns = ['date', 'tag', 'tag_count']

    return df4
