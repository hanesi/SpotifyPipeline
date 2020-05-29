import spotipy
import spotipy.util as util
import pymysql
import sqlalchemy
import requests
import sys
import pandas as pd
import json
from pandas.io.json import json_normalize
import os

un = os.environ['un']
scope = 'user-read-recently-played'
cid = os.environ['cid']
csid = os.environ['csid']
redr = r'http://localhost:8888/callback/'
host = os.environ['host']
db = os.environ['db']
user = os.environ['user']
password = os.environ['password']

def lambda_handler(event,context):
    print('Connecting...')
    connection = pymysql.connect(
                                host=host,
                                port = 3306,
                                user = user,
                                password=password,
                                db = db,
                                connect_timeout = 400
                                 )
    #Create engine
    print('Creating Engine')
    engine = sqlalchemy.create_engine('mysql+pymysql://'+user+':'+password+'@'+host+':3306/'+db)

    token = util.prompt_for_user_token(un,scope,cid,csid,redr)
    sp = spotipy.Spotify(auth=token)
    results = sp.current_user_recently_played()

    print('Extracting Artist Names')
    artist_list = []
    for i in results['items']:
        artist = i['track']['artists'][0]['name']
        artist_list.append(artist)

    artistIDs = set([i['track']['artists'][0]['id'] for i in results['items']])

    print('Generating Artist and Genre Tables')
    artistInfo = []
    genreMapper = []
    for i in artistIDs:
        artistDict = {}
        genreDict = {}
        rawInfo = sp.artist(i)
        artistDict['artistID'] = rawInfo['uri']
        artistDict['artistName'] = rawInfo['name']
        artistDict['artistPopularity'] = rawInfo['popularity']
        artistDict['artistFollowers'] = rawInfo['followers']['total']
        artistInfo.append(artistDict)

        ID = rawInfo['uri']
        genres = rawInfo['genres']
        for j in genres:
            ok = {}
            ok['ID'] = ID
            ok['genre'] = j
            genreMapper.append(ok)

    print('Writing genre/artist Tables')
    dfArtists = pd.DataFrame(artistInfo)
    dfArtists = dfArtists[['artistID', 'artistName', 'artistPopularity', 'artistFollowers']]
    dfGenreMapper = pd.DataFrame(genreMapper)
    dfGenreMapper = dfGenreMapper[['ID', 'genre']]

    dfArtists.to_sql(name = 'ArtistInfo', con = engine, schema = 'spotifydb1',if_exists='append',index=False)
    dfGenreMapper.to_sql(name = 'GenreMapper', con = engine, schema = 'spotifydb1',if_exists='append',index=False)

    cursor = connection.cursor()
    cursor.execute("SELECT max(played_at) FROM spotifydb1.StreamingLog;")
    max_date = cursor.fetchall()
    max_date = max_date[0][0].strftime('%Y-%m-%d %H:%M:%S')

    print('Dropping DUPs from Artist/Genre Tables')
    cursor.execute("alter ignore table ArtistInfo add unique(artistID);")
    cursor.execute("ALTER TABLE `spotifydb1`.`ArtistInfo` DROP INDEX `artistID`;")

    cursor.execute("alter ignore table GenreMapper add unique(ID, genre);")
    cursor.execute("ALTER TABLE `spotifydb1`.`GenreMapper` DROP INDEX `ID`;")

    print('Creating StreamingLog Table')
    test = json_normalize(results['items'])
    test = test.drop(['track.available_markets','track.album.available_markets'],axis =1)
    test['artist'] = artist_list
    test['played_at'] = pd.to_datetime(test['played_at'], format='%Y-%m-%d %H:%M:%S')
    test['played_at'] = test['played_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    dfUpload = test[['played_at','track.album.album_type','track.album.name','track.album.release_date',
                     'track.album.total_tracks','track.track_number','artist','track.name','track.popularity',
                     'track.duration_ms','track.is_local']]

    dfUpload = dfUpload[dfUpload['played_at'] > max_date]

    print('Uploading StreamingLog')
    if len(dfUpload) == 0:
        connection.close()
    else:
        dfUpload.to_sql(name = 'StreamingLog', con = engine, schema = 'spotifydb1',if_exists='append',index=False)
        connection.close()
