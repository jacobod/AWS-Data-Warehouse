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

staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs BIGINT,
    artist_id VARCHAR(200) NOT NULL,
    artist_latitude DOUBLE PRECISION,
    artist_longititude DOUBLE PRECISION,
    artist_location VARCHAR(200),
    artist_name VARCHAR(200) NOT NULL,
    song_id VARCHAR(200) NOT NULL,
    title VARCHAR(200) NOT NULL,
    duration DOUBLE PRECISION,
    year BIGINT,
)

""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id BIGINT IDENTITY(0,1) NOT NULL,
    artist VARCHAR(200) NOT NULL,
    auth VARCHAR(100),
    first_name VARCHAR(100),
    gender VARCHAR(100),
    item_in_session BIGINT NOT NULL,
    last_name VARCHAR(100),
    length DOUBLE PRECISION,
    level VARCHAR(100),
    location VARCHAR(200),
    method VARCHAR(100),
    page VARCHAR(100),
    registration DOUBLE PRECISION,
    sessionId BIGINT NOT NULL,
    song VARCHAR(200) NOT NULL,
    status BIGINT,
    ts BIGINT,
    user_agent VARCHAR(200),
    user_id BIGINT NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    id BIGINT IDENTITY(0,1), 
    start_time TIMESTAMP NOT NULL,
    user_id BIGINT NOT NULL, 
    level VARCHAR(20), 
    song_id VARCHAR(100), 
    artist_id VARCHAR(100), 
    session_id BIGINT NOT NULL, 
    location VARCHAR(100),
    user_agent VARCHAR(100)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT PRIMARY KEY NOT NULL, 
    first_name varchar,
    last_name varchar, 
    gender varchar, 
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY NOT NULL,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int, 
    duration double precision
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY NOT NULL,
    name varchar,
    location varchar,
    lattitude double precision,
    longitude double precision	
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int, 
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role '{}'
    region 'us-west-2'
    format as json 'auto';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role '{}'
    region 'us-west-2'
    format as json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (id, start_time,user_id, level, song_id,
                           artist_id, session_id,location, user_agent)
    
    SELECT 
        '1970-01-01'::date + ts/1000 * interval '1 second' AS start_time,
        userId                                             AS user_id, 
        level, 
        staging_songs.song_id,
        staging_songs.artist_id,
        sessionId                                          AS session_id,
        location,
        userAgent                                          AS user_agent
    FROM staging_events
    JOIN staging_songs
    ON staging_events.song = starging_songs.title
    AND staging_events.artist = staging_songs.artist_name
    AND staging_events.length = staging_songs.duration
    WHERE staging_events.page = 'NextSong'
    ;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    ON CONFLICT (user_id) DO UPDATE
        SET level = CASE WHEN users.level != EXCLUDED.level
                         THEN EXCLUDED.level
                         ELSE users.level
                    END
                    
    SELECT 
        userId, 
        firstName, 
        lastName, 
        gender,
        level 
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    ON CONFLICT(song_id) DO NOTHING
    
    SELECT 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    ON CONFLICT(artist_id) DO NOTHING
    
    SELECT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_lattitude, 
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    ON CONFLICT(start_time) DO NOTHING
    
    SELECT 
        start_time,
        EXTRACT(hr FROM start_time)                        AS hour,
        EXTRACT(d FROM start_time)                         AS day,
        EXTRACT(w FROM start_time)                         AS week,
        EXTRACT(mon FROM start_time)                       AS month,
        EXTRACT(yr FROM start_time)                        AS year,
        EXTRACT(dow FROM start_time)                       AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
