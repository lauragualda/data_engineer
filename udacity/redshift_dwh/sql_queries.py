import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_events (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TEMPORARY TABLE IF NOT EXISTS staging_songs (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR
    itemInSession INTEGER,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DOUBLE PRECISION,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts NUMERIC,
    userAgent VARCHAR,
    userId INTEGER
    );
""")



songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration DOUBLE PRECISION
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER, 
    month INTEGER, 
    year INTEGER,
    weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/log_data/2018/11/'
CREDENTIALS 'aws_iam_role={}'
gzip delimiter ';' compupdate off region
REGION 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data/A/'
CREDENTIALS 'aws_iam_role={}'
gzip delimiter ';' compupdate off region
REGION 'us-west-2';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  timestamp 'epoch' + songs.ts/1000 * interval '1 second', 
        e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e
LEFT JOIN staging_songs s 
    ON  e.song = s.title
    AND e.artist = s.artist_name
    AND e.length = duration
""")


user_table_insert = (""" 
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_songs
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
JOIN 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, name, location, latitude, longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
     extract(hour from start_time) as hour, 
     extract(day from start_time) as day, 
     extract(week from start_time) as week, 
     extract(month from start_time) as month, 
     extract(year from start_time) as year, 
     extract(weekday from start_time) as weekday
FROM (SELECT distinct timestamp 'epoch' + ts/1000 * interval '1 second' as start_time FROM staging);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
