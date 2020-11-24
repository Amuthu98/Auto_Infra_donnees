from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import os
import datetime
import pandas as pd
from pprint import pprint

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="855c14116eb64f31a93d97374c6aa1b0",
                                                           client_secret="67e8c03c44ab45d294ddb4227c86aab8"))

today = datetime.date.today()

#Open CSV WeeklyNewRelease
df = pd.read_csv('Data/WeeklyNewRelease.csv', sep=';', header=0, encoding='utf-8')
AlbumsIdList = df.album_id

#Loop for get data and put them in csv for every album/single in WeeklyNewRelease.csv
for album in AlbumsIdList:
    urn = 'spotify:album:'+album

    album = sp.album(urn)

    #Get all the tracks of the album and put them in one row
    tracks = album['tracks']
    TrackList = []
    for i, item in enumerate(tracks['items']):
        TrackList.append(str(item['track_number']) + ' ' + item['name'])
    TrackList = ', '.join(TrackList)

    #Get all available markets and put them in a row
    available_markets = ', '.join(album['available_markets'])

    #Check for special char in the album/artist name and delete them
    Special_char = ['#', '*', '<', '>', '?', '/', '\\', '|', ':', '"']
    if any(ele in album['name'] for ele in Special_char):
        for char in Special_char:
            album['name'] = album['name'].replace(char, '')
    if any(ele in album['artists'][0]['name'] for ele in Special_char):
        for char in Special_char:
            album['artists'][0]['name'] = album['artists'][0]['name'].replace(char, '')

    #Check if folder already exist (yes : continue / no : create folder)
    if not os.path.exists('Data/MetaAlbum/'+album['name']+' - '+album['artists'][0]['name']):
        os.makedirs('Data/MetaAlbum/'+album['name']+' - '+album['artists'][0]['name'])

    #Open CSV MetaAlbum
    MetaAlbum = open('Data/MetaAlbum/'+album['name']+' - '+album['artists'][0]['name']+'/'+album['id']+'.csv', 'a', encoding='utf-8')

    #if the MetaAlbum csv is empty create the header
    if os.stat('Data/MetaAlbum/'+album['name']+' - '+album['artists'][0]['name']+'/'+album['id']+'.csv').st_size == 0:
        MetaAlbum.write('album_date_extract;album_type;album_id;album_name;album_release_date;album_popularity;album_label;album_image;artist_id;artist_name;tracks;available_markets\n')

    #fill the csv with data
    MetaAlbum.write(str(today) + ';' + album['album_type'] + ';' + album['id'] + ';' + album['name'] + ';' + album['release_date'] + ';' + str(album['popularity']) + ';' + album['label'] + ';' + album['images'][0]['url'] + ';' + album['artists'][0]['id'] + ';' + album['artists'][0]['name'] + ';' + TrackList + ';' +available_markets +'\n')
    MetaAlbum.close()
