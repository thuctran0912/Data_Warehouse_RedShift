import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES TO RESET DATABASE AND TEST THE ETL 

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES: 2 STAGING TABLES (EVENTS AND SONGS), 1 FACT TABLE (SONGPLAY) AND 4 DIMENSION TABLES (USERS, SONGS, ARTISTS, TIME)
print("Creating Staging Events table")
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_event 
(
    artist          TEXT,
    auth            VARCHAR(15),
    first_name      VARCHAR(15),
    gender          VARCHAR(15),
    itemInSession   INT,
    last_name       VARCHAR(15),
    length          DECIMAL,
    level           VARCHAR(4),
    location        TEXT,
    method          VARCHAR(3),
    page            TEXT,
    registration    TEXT,
    sessionID       TEXT,
    song            TEXT,
    status          VARCHAR(3),
    ts              TIMESTAMP,
    userAgent       TEXT,
    userId          TEXT 
);                                       
""")
print("Creating Staging Song table")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_song (
    num_songs         BIGINT,
    artist_id         TEXT,
    artist_latitude   DECIMAL, 
    artist_longitude  DECIMAL,
    artist_location   TEXT,  
    artist_name       TEXT,
    song_id           TEXT,
    title             TEXT,
    duration          DECIMAL,    
    year              INT                                                                                                                                                                           
);
""")

print("Create songplay table")
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id   INT IDENTITY(0,1) PRIMARY KEY, 
    start_time    TIMESTAMP NOT NULL DISTKEY SORTKEY, 
    user_id       TEXT NOT NULL, 
    level         VARCHAR(4) NOT NULL, 
    song_id       TEXT, 
    artist_id     TEXT, 
    session_id    TEXT, 
    location      TEXT, 
    user_agent    TEXT                                     
) DISTSTYLE KEY;
""")
                         
print("Create user table")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id       TEXT PRIMARY KEY SORTKEY, 
    first_name    VARCHAR(15),
    last_name     VARCHAR(15), 
    gender        VARCHAR(15),
    level         VARCHAR(4)                 
) DISTSTYLE ALL;
""")
                     
print("Create song table")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id       TEXT PRIMARY KEY SORTKEY, 
    title         TEXT NOT NULL, 
    artist_id     TEXT NOT NULL, 
    year          INT NOT NULL, 
    "duration"      DECIMAL NOT NULL                   
) DISTSTYLE ALL;
""")
                     
print("Create artist table")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id     TEXT PRIMARY KEY SORTKEY, 
    name          TEXT NOT NULL, 
    location      TEXT, 
    latitude      DECIMAL, 
    longitude     DECIMAL                     
) DISTSTYLE ALL;
""")
                       

print("Create time table")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time    TIMESTAMP PRIMARY KEY SORTKEY, 
    hour          INT NOT NULL, 
    day           INT NOT NULL, 
    week          INT NOT NULL, 
    month         INT NOT NULL, 
    year          INT NOT NULL, 
    weekday       INT NOT NULL                   
) DISTSTYLE ALL;
""")

# INSERT DATA INTO STAGING TABLES

staging_events_copy = ("""
COPY {} FROM {}
IAM_ROLE {}
JSON {} REGION '{}'
""").format(
    'staging_event',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']["ARN"],
    config["S3"]["LOG_JSONPATH"],
    config["CLUSTER"]["REGION"]
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE {}
    JSON 'auto' region '{}';
""").format(
    'staging_song',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# INSERT DATA FROM THE STAGING TABLES TO THE STAR SCHEMA TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second',
        se.userId AS user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionID AS session_id,
        se.location,
        se.userAgent AS user_agent
    FROM staging_event se
    LEFT JOIN staging_song ss ON 
        se.song = ss.title AND
        se.artist = ss.artist_name
    WHERE se.page = 'NextSong'                  
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userid AS user_id, 
        first_name,
        last_name,
        gender,
        level
    FROM staging_event
    WHERE userid IS NOT NULL
    AND page = 'NextSong'             
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_song
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS Longitude
    FROM staging_song
""")

time_table_insert = ("""
    INSERT INTO time (start_time,hour,day,week,month,year,weekday)
        SELECT DISTINCT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
