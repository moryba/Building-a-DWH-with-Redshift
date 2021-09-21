import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
event_id BIGINT IDENTITY(0,1),
artist TEXT,
auth TEXT,
firstname TEXT,
gender TEXT,
iteminsession TEXT,
lastname TEXT,
length DECIMAL,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration TEXT,
sessionid INTEGER,
song TEXT,
status INTEGER,
ts BIGINT,
useragent TEXT,
userid INTEGER);""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
num_songs INTEGER,
artist_id TEXT,
artist_latitude TEXT,
artist_longitude TEXT,
artist_location TEXT,
artist_name TEXT,
song_id TEXT,
title TEXT,
duration DECIMAL,
year INTEGER); """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (\
songplay_id INTEGER IDENTITY(0,1), \
start_time TIMESTAMP NOT NULL, \
userid INTEGER NOT NULL, \
level TEXT, \
song_id TEXT, \
artist_id TEXT, \
session_id TEXT, \
location TEXT, \
user_agent TEXT, \
PRIMARY KEY (songplay_id));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (\
userid INTEGER, \
first_name TEXT, \
last_name TEXT, \
gender TEXT, \
level TEXT,
PRIMARY KEY (userid));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (\
song_id TEXT, \
title TEXT, \
artist_id TEXT, \
year INTEGER, \
duration DECIMAL,
PRIMARY KEY (song_id));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (\
artist_id TEXT, \
name TEXT, \
location TEXT, \
latitude DECIMAL, \
longitude DECIMAL, \
PRIMARY KEY (artist_id));""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (\
start_time TIMESTAMP, \
hour INTEGER, \
day INTEGER, \
week INTEGER, \
month INTEGER, \
year INTEGER, \
weekday INTEGER, \
PRIMARY KEY (start_time));""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}' 
    format as json {} 
    STATUPDATE ON 
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'  
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, userid, level, song_id, artist_id,
session_id, location, user_agent) \
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time, \
se.userid, \
se.level, \
so.song_id, \
so.artist_id, \
se.sessionid, \
se.location, \
se.useragent \
FROM staging_events se JOIN staging_songs so ON (se.artist = so.artist_name AND se.song = so.title AND se.length = so.duration)\
WHERE page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (userid, first_name, last_name, gender, level) \
SELECT userid , \
firstname ,\
lastname ,\
gender ,\
level FROM staging_events
WHERE page='NextSong';""")

song_table_insert = ("""INSERT INTO songs  (song_id, title, artist_id, year, duration) \
SELECT DISTINCT song_id ,\
title ,\
artist_id, \
year, \
duration FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
SELECT DISTINCT artist_id, \
artist_name, \
artist_location, \
artist_latitude, \
artist_longitude FROM staging_songs""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
SELECT DISTINCT start_time ,\
EXTRACT(hour FROM start_time) , \
EXTRACT(day FROM start_time),\
EXTRACT(week FROM start_time), \
EXTRACT(month FROM start_time), \
EXTRACT(year FROM start_time), \
EXTRACT(weekday FROM start_time) FROM songplays""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
