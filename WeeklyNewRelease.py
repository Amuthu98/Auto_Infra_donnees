import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
import os

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="855c14116eb64f31a93d97374c6aa1b0",
                                                           client_secret="67e8c03c44ab45d294ddb4227c86aab8"))

response = sp.new_releases()

#Open CSV
WeeklyNewRelease = open('Data/WeeklyNewRelease.csv', 'a', encoding='utf-8')

#if the WeeklyNewRelease csv is empty create the header
if os.stat('Data/WeeklyNewRelease.csv').st_size == 0:
    WeeklyNewRelease.write('album_type;album_id;album_name;album_release_date;album_release_week;artist_id;artist_names\n')

#Set today date and today week number
today = datetime.date.today()
today_week_num = today.isocalendar()[1] - 1

#Export new release album / single in .csv
while response:
    albums = response['albums']
    for i, item in enumerate(albums['items']):
        year = item['release_date'][:4]
        month = item['release_date'][5:7]
        day = item['release_date'][8:10]
        if year and month and day is not None:
            if today_week_num == datetime.date(int(year), int(month), int(day)).isocalendar()[1]:
                WeeklyNewRelease.write(item['album_type'] + ';' + item['id'] + ';' + item['name'] + ';' + item['release_date'] + ';' + str(datetime.date(int(year), int(month), int(day)).isocalendar()[1]) + ';' + item['artists'][0]['id'] + ';' + item['artists'][0]['name'] + '\n')

    if albums['next']:
        response = sp.next(albums)
    else:
        response = None

WeeklyNewRelease.close()
