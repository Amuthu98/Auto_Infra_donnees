import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import datetime
import pandas as pd

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="855c14116eb64f31a93d97374c6aa1b0",
                                                           client_secret="67e8c03c44ab45d294ddb4227c86aab8"))

today = datetime.date.today()
today_week_num = today.isocalendar()[1] - 1

#New release
for x in range(0, 2000, 50):
    response = sp.search(q='tag:new', type='album', limit=50, offset=x)

    #Open CSV
    WeeklyNewRelease = open('Data/WeeklyNewRelease.csv', 'a', encoding='utf-8')

    #if the WeeklyNewRelease csv is empty create the header
    if os.stat('Data/WeeklyNewRelease.csv').st_size == 0:
        WeeklyNewRelease.write('album_type;album_id;album_name;album_release_date;album_release_week;artist_id;artist_names\n')

    albums = response['albums']
    for i, item in enumerate(albums['items']):
        year = item['release_date'][:4]
        month = item['release_date'][5:7]
        day = item['release_date'][8:10]
        if year and month and day is not None:
            if datetime.date(int(year), int(month), int(day)).isocalendar()[1] == today_week_num:
                Special_char = [';']
                if any(ele in item['name'] for ele in Special_char):
                    for char in Special_char:
                        item['name'] = item['name'].replace(char, '')
                WeeklyNewRelease.write(item['album_type'] + ';' + item['id'] + ';' + item['name'] + ';' + item['release_date'] + ';' + str(datetime.date(int(year), int(month), int(day)).isocalendar()[1]) + ';' + item['artists'][0]['id'] + ';' + item['artists'][0]['name'] + '\n')

    WeeklyNewRelease.close()

#new release small artists (-10% popularity)
for x in range(0, 2000, 50):
    response = sp.search(q='tag:hipster', type='album', limit=50, offset=x)

    #Open CSV
    WeeklyNewRelease = open('Data/WeeklyNewRelease.csv', 'a', encoding='utf-8')

    #if the WeeklyNewRelease csv is empty create the header
    if os.stat('Data/WeeklyNewRelease.csv').st_size == 0:
        WeeklyNewRelease.write('album_type;album_id;album_name;album_release_date;album_release_week;artist_id;artist_names\n')

    albums = response['albums']
    for i, item in enumerate(albums['items']):
        year = item['release_date'][:4]
        month = item['release_date'][5:7]
        day = item['release_date'][8:10]
        if year and month and day is not None:
            if datetime.date(int(year), int(month), int(day)).isocalendar()[1] == today_week_num:
                Special_char = [';']
                if any(ele in item['name'] for ele in Special_char):
                    for char in Special_char:
                        item['name'] = item['name'].replace(char, '')
                WeeklyNewRelease.write(item['album_type'] + ';' + item['id'] + ';' + item['name'] + ';' + item['release_date'] + ';' + str(datetime.date(int(year), int(month), int(day)).isocalendar()[1]) + ';' + item['artists'][0]['id'] + ';' + item['artists'][0]['name'] + '\n')

    WeeklyNewRelease.close()

#Delete duplicate rows
data = pd.read_csv("Data/WeeklyNewRelease.csv", sep=';', header=0, encoding='utf-8')
data.drop_duplicates(subset=['album_id'], inplace=True, keep='first')
data.to_csv('Data/WeeklyNewRelease.csv', index=False, sep=';')
