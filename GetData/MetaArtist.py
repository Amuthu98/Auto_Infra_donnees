from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import datetime
import pandas as pd
import os
from pprint import pprint

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="855c14116eb64f31a93d97374c6aa1b0",
                                                           client_secret="67e8c03c44ab45d294ddb4227c86aab8"))

today = datetime.date.today()

#Open CSV WeeklyNewRelease
df = pd.read_csv('Data/WeeklyNewRelease.csv', sep=';', header=0, encoding='utf-8')
ArtistsIdList = df.artist_id

#Loop for get data and put them in csv for every artist in WeeklyNewRelease.csv
for artist in ArtistsIdList:
    urn = 'spotify:artist:'+artist

    artist = sp.artist(urn)

    # Get all genres and put them in a row
    genres = ', '.join(artist['genres'])

    # Check for special char in the artist name and delete them
    Special_char = ['#', '*', '<', '>', '?', '/', '\\', '|', ':', '"']
    if any(ele in artist['name'] for ele in Special_char):
        for char in Special_char:
            artist['name'] = artist['name'].replace(char, '')

    # Check if folder already exist (yes : continue / no : create folder)
    if not os.path.exists('Data/MetaArtist/' + artist['name']):
        os.makedirs('Data/MetaArtist/' + artist['name'])

    # Open CSV MetaAlbum
    MetaArtist = open('Data/MetaArtist/' + artist['name'] + '/' + artist['id'] + '.csv', 'a', encoding='utf-8')

    # if the MetaAlbum csv is empty create the header
    if os.stat('Data/MetaArtist/' + artist['name'] + '/' + artist['id'] + '.csv').st_size == 0:
        MetaArtist.write('artist_date_extract;artist_id;artist_name;genres;artist_followers;artist_popularity;artist_image\n')

    # fill the csv with data
    MetaArtist.write(str(today) + ';' + artist['id'] + ';' + artist['name'] + ';' + genres + ';' + str(artist['followers']['total']) + ';' + str(artist['popularity']) + ';' + artist['images'][0]['url'] +'\n')
    MetaArtist.close()