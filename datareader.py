# datareader.py takes the survey csv and creates another csv with
# everyone's songs and song features labelled with their username

from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import pandas as pd
import numpy as np
import math
import json
from joblib import delayed, Parallel
input = 'datacollection_042623.csv'


# update access keys here for Spotify API here :)
client_id = 'f7661e34c0924317a8b928c0f977e6a7'
client_secret = 'd068ec3f0a98490e9c32be820916e934'

# sp is how we interface with the Spotify API
# read the docs here: https://spotipy.readthedocs.io/en/2.22.1/
sp = spotipy.Spotify(
    client_credentials_manager=SpotifyClientCredentials(client_id, client_secret))


def user_playlist_tracks_full(user, playlist_id=None, fields=None):
    """ Get full details of the tracks of a playlist owned by a user.
        https://developer.spotify.com/documentation/web-api/reference/playlists/get-playlists-tracks/

        Parameters:
            - user - the id of the user
            - playlist_id - the id of the playlist
            - fields - which fields to return
    """

    # first run through also retrieves total no of songs in library
    response = sp.user_playlist_tracks(
        user, playlist_id, fields=None, limit=100)
    results = response["items"]

    # subsequently runs until it hits the user-defined limit or has read all songs in the library
    while len(results) < response["total"]:
        response = sp.user_playlist_tracks(
            user, playlist_id, fields=fields, limit=100, offset=len(results)
        )
        results.extend(response["items"])

    return results


def get_tracks_full(songs):
    results = []
    while len(results) < len(songs):
        response = (sp.tracks(songs[len(results):len(results) + 50]))['tracks']
        response = list(filter(None, response))
        results.extend(response)

    return results


def get_audio_features_full(songs):
    results = []
    while len(results) < len(songs):
        response = sp.audio_features(songs[len(results):len(results) + 100])
        response = list(filter(None, response))
        results.extend(response)
    return results


def get_data_from_user(username):
    print("Started " + username + "...")
    playlists = []
    LIMIT = 20

    query = sp.user_playlists(username, limit=LIMIT)

    n = 0

    while len(query['items']) != 0:
        n += LIMIT
        for i in query['items']:
            playlists.append(
                {'title': i['name'], 'id': i['id'], 'length': i['tracks']['total']})
        if LIMIT == len(query['items']):
            query = sp.user_playlists(username, limit=LIMIT, offset=n)
        else:
            break

    songs = []

    for p in playlists:

        results = sp.playlist(p['id'])

        extended_results = user_playlist_tracks_full(
            user=username, playlist_id=results['id'])  # more than 100

        for r in extended_results:
            try:
                songs.append(r['track']['id'])
            except:
                pass

    songs = list(filter(None, songs))

    # If no valid songs, return empty dataframe
    if (len(songs) == 0):
        return pd.DataFrame()

    songs_meta = get_tracks_full(songs)

    song_info = {'id': [], 'title': [], 'artist': [], 'album': [],
                 'explicit': [], 'popularity': []}

    for i in songs_meta:
        # append id to id column
        song_info['id'].append(i['id'])

        # append song title to title column
        song_info['title'].append(i['name'])

        # append artists to artist column
        s = ', '
        artists = s.join([name['name'] for name
                         in i['artists']])
        song_info['artist'].append(artists)

        # append album title to album column
        song_info['album'].append(i['album']['name'])

        # append explicit info to explicit column
        song_info['explicit'].append(i['explicit'])

        # append popularity info to popularity column
        song_info['popularity'].append(i['popularity'])

    # convert song_info dictionary -> Pandas DataFrame
    song_info_df = pd.DataFrame.from_dict(song_info)

    song_info_df = song_info_df.drop_duplicates(subset=['id'])
    song_info_df = song_info_df.dropna()

    features_df = pd.DataFrame.from_dict(get_audio_features_full(songs))
    features_df = features_df.drop_duplicates(subset=['id'])
    features_df = features_df.dropna()

    df = song_info_df.merge(features_df)
    df = df.drop(columns=['album', 'key', 'mode', 'type', 'uri',
                 'track_href', 'analysis_url', 'duration_ms', 'time_signature'])
    df.insert(0, 'username', username)
    print("Finished " + username)
    return df

# General public playlist scraping


def print_general():
    students_df = pd.read_csv(input)

    # get rid of some silly columns
    students_df = students_df.drop(columns=['StartDate', 'EndDate',
                                            'Status', 'IPAddress', 'Duration (in seconds)', 'RecordedDate',
                                            'ResponseId', 'RecipientLastName', 'RecipientFirstName',
                                            'RecipientEmail', 'ExternalReference', 'LocationLatitude',
                                            'LocationLongitude', 'DistributionChannel', 'UserLanguage'])

    # drop anyone who didn't link a profile properly
    students_df = students_df.dropna(axis=0, subset=['Q9'])
    students_df = students_df[students_df["Q9"].str.contains(
        'https://open.spotify.com/user')]

    # print our questions, then drop row of questions
    students_df = students_df.tail(-1)

    # reset index
    students_df = students_df.reset_index()

    # ok what do we have now
    print('Number of linked spotify profiles: ' + str(students_df.shape[0]))

    # let's get a list of spotify usernames

    usernames = students_df['Q9']

    stripped_usernames = []

    for i in usernames:
        i = i.replace("https://open.spotify.com/user/", "")
        i = i.split('?si', 1)[0]
        stripped_usernames.append(i)

        i = + 1

    print('Number of usernames: ' + str(len(stripped_usernames)))

    # shadow wizard money gang multithreading sorcery
    # https://stackoverflow.com/questions/5236364/how-to-parallelize-list-comprehension-calculations-in-python
    frames = Parallel(n_jobs=8)(delayed(get_data_from_user)(u)
                                for u in stripped_usernames)

    frames = list(filter(lambda x: not x.empty, frames))
    all_data = pd.concat(frames)

    all_data.to_csv('all_cornell.csv', index=False)


def get_playlist_data(username, playlist_id):
    songs = []

    extended_results = user_playlist_tracks_full(
        user=username, playlist_id=playlist_id)  # more than 100

    for r in extended_results:
        try:
            songs.append(r['track']['id'])
        except:
            pass

    songs = list(filter(None, songs))

    # If no valid songs, return empty dataframe
    if (len(songs) == 0):
        return pd.DataFrame()

    songs_meta = get_tracks_full(songs)

    song_info = {'id': [], 'title': [], 'artist': [], 'album': [],
                 'explicit': [], 'popularity': []}

    for i in songs_meta:
        # append id to id column
        song_info['id'].append(i['id'])

        # append song title to title column
        song_info['title'].append(i['name'])

        # append artists to artist column
        s = ', '
        artists = s.join([name['name'] for name
                         in i['artists']])
        song_info['artist'].append(artists)

        # append album title to album column
        song_info['album'].append(i['album']['name'])

        # append explicit info to explicit column
        song_info['explicit'].append(i['explicit'])

        # append popularity info to popularity column
        song_info['popularity'].append(i['popularity'])

    # convert song_info dictionary -> Pandas DataFrame
    song_info_df = pd.DataFrame.from_dict(song_info)

    song_info_df = song_info_df.drop_duplicates(subset=['id'])
    song_info_df = song_info_df.dropna()

    features_df = pd.DataFrame.from_dict(get_audio_features_full(songs))
    features_df = features_df.drop_duplicates(subset=['id'])
    features_df = features_df.dropna()

    df = song_info_df.merge(features_df)
    df = df.drop(columns=['album', 'key', 'mode', 'type', 'uri',
                 'track_href', 'analysis_url', 'duration_ms', 'time_signature'])
    df.insert(0, 'username', username)

    return df


def parse_playlist(question):
    # Gym playlists
    gym_df = pd.read_csv(input)
    # get rid of some silly columns
    gym_df = gym_df.drop(columns=['StartDate', 'EndDate',
                                  'Status', 'IPAddress', 'Duration (in seconds)', 'RecordedDate',
                                  'ResponseId', 'RecipientLastName', 'RecipientFirstName',
                                            'RecipientEmail', 'ExternalReference', 'LocationLatitude',
                                            'LocationLongitude', 'DistributionChannel', 'UserLanguage'])

    # drop anyone who didn't link a profile properly
    gym_df = gym_df.dropna(axis=0, subset=['Q9'])
    gym_df = gym_df[gym_df["Q9"].str.contains(
        'https://open.spotify.com/user')]
    gym_df = gym_df.dropna(axis=0, subset=[question])
    gym_df = gym_df[gym_df[question].str.contains(
        'https://open.spotify.com/playlist')]
    gym_df = gym_df.tail(-1)
    gym_df = gym_df.reset_index()

    # ok what do we have now
    print('Number of linked spotify profiles: ' + str(gym_df.shape[0]))

    # let's get a list of spotify usernames

    usernames = gym_df['Q9']

    stripped_usernames = []

    for i in usernames:
        i = i.replace("https://open.spotify.com/user/", "")
        i = i.split('?si', 1)[0]
        stripped_usernames.append(i)

        i = + 1

    print('Number of usernames: ' + str(len(stripped_usernames)))

    playlists = gym_df[question]

    stripped_playlists = []

    for i in playlists:
        i = i.replace("https://open.spotify.com/playlist/", "")
        i = i.split('?si', 1)[0]
        stripped_playlists.append(i)

        i = + 1

    print('Number of playlists: ' + str(len(stripped_playlists)))

    frames = Parallel(n_jobs=8)(delayed(get_playlist_data)(u, p)
                                for (u, p) in zip(stripped_usernames, stripped_playlists))

    frames = list(filter(lambda x: not x.empty, frames))
    all_data = pd.concat(frames)

    dict = {'Q11_1': 'gym', 'Q11_2': 'study',
            'Q11_3': 'party', 'Q11_4': 'sleep', 'Q11_5': 'commute'}
    all_data.to_csv('all_cornell_' + dict[question] + '.csv', index=False)


# print_general()
parse_playlist('Q11_1')  # Gym
parse_playlist('Q11_2')  # Study
parse_playlist('Q11_3')  # Party
parse_playlist('Q11_4')  # Sleep
parse_playlist('Q11_5')  # Commute
