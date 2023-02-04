import configparser

# CONFIG

# Read configuration from dwh.cfg file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# Drop the staging_events table if it exists
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events CASCADE;'

# Drop the staging_songs table if it exists
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs CASCADE;'

# Drop the songplays table if it exists
songplay_table_drop = 'DROP TABLE IF EXISTS songplays CASCADE;'

# Drop the users table if it exists
user_table_drop = 'DROP TABLE IF EXISTS users CASCADE;'

# Drop the songs table if it exists
song_table_drop = 'DROP TABLE IF EXISTS songs CASCADE;'

# Drop the artists table if it exists
artist_table_drop = 'DROP TABLE IF EXISTS artists CASCADE;'

# Drop the time table if it exists
time_table_drop = 'DROP TABLE IF EXISTS time CASCADE;'


# CREATE TABLES

# Create the staging_events table with the following columns:
# artist_name, auth, first_name, gender, item_in_session, last_name, length,
# level, location, method, page, registration, session_id, song, status,
# ts, user_agent, user_id
staging_events_table_create = '''
CREATE TABLE IF NOT EXISTS staging_events (
    artist_name      VARCHAR(MAX),
    auth             VARCHAR(255),
    first_name       VARCHAR(255),
    gender           VARCHAR(255),
    item_in_session  INT4,
    last_name        VARCHAR(255),
    length           NUMERIC,
    level            VARCHAR(255),
    location         VARCHAR(255),
    method           VARCHAR(255),
    page             VARCHAR(255),
    registration     VARCHAR(255),
    session_id       INT4,
    song             VARCHAR(255),
    status           VARCHAR(255),
    ts               BIGINT,
    user_agent       VARCHAR(255),
    user_id          VARCHAR(255)
);
'''

# Create the staging_songs table with the following columns:
# num_songs, artist_id, artist_latitude, artist_longitude, artist_location,
# artist_name, song_id, title, duration, year
staging_songs_table_create = '''
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INT,
    artist_id        VARCHAR(255),
    artist_latitude  NUMERIC,
    artist_longitude NUMERIC,
    artist_location  VARCHAR(255),
    artist_name      VARCHAR(MAX),
    song_id          VARCHAR(255),
    title            VARCHAR(255),
    duration         NUMERIC,
    year             INT
);
'''

# Create the songplays table with the following columns:
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id,
# location, user_agent
# songplay_id is the primary key and is an auto-incrementing column
# start_time is a required field and references the start_time column in the time table
# user_id is a required field and references the user_id column in the users table
# song_id is a required field and references the song_id column in the songs table
# artist_id is a required field and references the artist_id column in the artists table
# A unique constraint has been added on the combination of start_time and user_id
songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id      int8 IDENTITY PRIMARY KEY,
        start_time       timestamp REFERENCES time(start_time) NOT NULL,
        user_id          varchar(255) REFERENCES users(user_id) NOT NULL,
        level            varchar(4),
        song_id          varchar(255) REFERENCES songs(song_id)  NOT NULL,
        artist_id        varchar(255) REFERENCES artists(artist_id)  NOT NULL,
        session_id       int4,
        location         varchar(255),
        user_agent       varchar(255),
        CONSTRAINT       uniqueness UNIQUE ("start_time","user_id")
    )
    DISTKEY (songplay_id)
    SORTKEY (user_id, song_id, artist_id)
    ;
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users(
        user_id         varchar(255) PRIMARY KEY,
        first_name      varchar(128),
        last_name       varchar(128),
        gender          char,
        level           varchar(4)    
    )
    DISTKEY (user_id)
    SORTKEY (user_id)
    ;
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs(
        song_id        varchar(255) PRIMARY KEY,
        title          varchar(255),
        artist_id      varchar(255),
        year           int4,
        duration       float8
    )
    DISTKEY (song_id)
    SORTKEY (artist_id)   
    ;
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists(
        artist_id   varchar(255) PRIMARY KEY,
        name        varchar(MAX),
        location    varchar(255),
        latitude    float8,
        longitude   float8    
    )
    DISTKEY (artist_id)
    SORTKEY (artist_id)
    ;
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time(
        start_time  timestamp PRIMARY KEY,
        hour        int4,
        day         int4,
        week        int4,
        month       int4,
        year        int4,
        weekday     int4    
    )
    DISTKEY (start_time)
    SORTKEY (start_time, year, month, week, day, weekday, hour)
    ;
    """
)


# STAGING TABLES

iam = config.get("IAM", "IAM_ROLE")
song_data = config.get("S3", "SONG_DATA")
log_data = config.get("S3", "LOG_DATA")
log_json_data = config.get("S3", "LOG_JSONPATH")

staging_events_copy = f"""
COPY staging_events 
FROM {log_data}
IAM_ROLE {iam}
JSON {log_json_data}
"""

staging_songs_copy = f"""
COPY staging_songs
FROM {song_data}
IAM_ROLE {iam}
JSON 'auto'
"""


# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    WITH song_artist as (
    SELECT 
        songs.artist_id,
        artists.name as artist_name,
        songs.title as song_name,
        songs.song_id as song_id,
        songs.duration as duration
    FROM songs
    INNER JOIN artists using (artist_id)
    )
    SELECT
        distinct date_add('ms', ts, '1970-01-01') as start_time,
        user_id,
        level,
        sa.song_id,
        sa.artist_id,
        sesion_id,
        location,
        user_agent
      FROM staging_events se
      INNER JOIN song_artist as sa 
      ON sa.artist_name=se.artist_name and sa.song_name=se.song AND sa.duration=se.length
      WHERE
        se.page = 'NextSong'
    """
)

song_table_insert = (
    """
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
        )
    SELECT 
        distinct song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECt song_id FROM songs)
    """
)

artist_table_insert = (
    """
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude  
        )
    SELECT    
        distinct artist_id,
        artist_name,
        artist_location,
        max(cast("artist_latitude" as float8)) AS "artist_latitude",
        max(cast("artist_longitude" as float8)) AS "artist_longitude"
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT artist_id FROM artists)
    GROUP BY 1,2,3
    """
)

# The following code inserts data into the 'users' table.
# It selects unique rows from the 'staging_events' table that are not already in the 'users' table.
user_table_insert = (
    """
    INSERT INTO users(
        user_id, # column to store the user id
        first_name, # column to store the first name
        last_name, # column to store the last name
        gender, # column to store the gender
        level # column to store the level
        )
    SELECT 
        distinct user_id, # select unique user_ids
        first_name, # select the first_name
        last_name, # select the last_name
        gender, # select the gender
        level # select the level
    FROM staging_events
    NATURAL INNER JOIN (SELECT user_id, max(ts) as ts FROM staging_events GROUP BY 1) as last_update
    WHERE user_id not in (SELECT user_id from users) # only select rows where the user_id is not already in the users table
    """
)

# The following code inserts data into the 'time' table.
# It converts the 'ts' column from the 'staging_events' table into a start_time and extracts various time components from it.
time_table_insert = ("""
INSERT INTO time(
    start_time, # column to store the start time
    hour, # column to store the hour
    day, # column to store the day
    week, # column to store the week
    month, # column to store the month
    year, # column to store the year
    weekday # column to store the weekday  
    )

WITH times AS(
    SELECT DATEADD('ms', ts, '19700101') as start_time # convert the 'ts' column from the 'staging_events' table into a start_time
    FROM staging_events
    GROUP BY 1
)

SELECT
    distinct start_time, # select unique start_times
    EXTRACT (hour FROM start_time) as hour, # extract the hour from the start_time
    EXTRACT (day FROM start_time) as day, # extract the day from the start_time
    EXTRACT (week FROM start_time) as week, # extract the week from the start_time
    EXTRACT (month FROM start_time) as month, # extract the month from the start_time
    EXTRACT (year FROM start_time) as year, # extract the year from the start_time
    EXTRACT (weekday FROM start_time) as weekday # extract the weekday from the start_time
FROM times
WHERE start_time NOT IN (SELECT start_time from time) # only select rows where the start_time is not already in the time table
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]
