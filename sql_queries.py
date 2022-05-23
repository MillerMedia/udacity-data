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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
    (    
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        user_id int
    )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id int identity(0,1) PRIMARY KEY,
        start_time timestamp,
        user_id varchar NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
    (
        user_id varchar PRIMARY KEY NOT NULL,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

# song_id, title, artist_id, year, duration
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
    (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
    (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES
# Opted for using IAM_ROLE variable instead of credentials based on this SO answer:
# https://stackoverflow.com/a/61945267/975592
staging_events_copy = ("""
    copy staging_events(
    artist, 
    auth, 
    firstName, 
    gender,
    itemInSession,
    lastName,
    length,
    level,
    location,
    method,
    page, 
    registration,
    sessionId,
    song,
    status,
    ts, 
    userAgent, 
    user_id
)
from {}
    format JSON as {}
    IAM_ROLE '{}'
""").format(config['S3']['LOG_DATA'], config['S3']['LOG_JSONPATH'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_songs (
    num_songs,
    artist_id,
    artist_latitude,
    artist_location,
    artist_longitude,
    artist_name,
    duration,
    song_id,
    title,
    year
    ) from {}
    IAM_ROLE '{}'
    format JSON as 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
# timestamp conversion reference: https://stackoverflow.com/a/54193360/975592
songplay_table_insert = ("""INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) SELECT distinct
    TIMESTAMP 'epoch' + se.ts * INTERVAL '1 second',
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
    FROM staging_events se
    INNER JOIN staging_songs ss ON
    se.artist = ss.artist_name AND
    se.song = ss.title
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users
    (
        user_id,
        first_name,
        last_name,
        gender,
        level
    ) SELECT distinct(user_id), firstName, lastName, gender, level
    FROM staging_events WHERE user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs
    (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) SELECT distinct song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists
    (
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time
    (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) SELECT distinct(TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time),
    extract(month from start_time),
    extract(year from start_time),
    extract(weekday from start_time)
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
