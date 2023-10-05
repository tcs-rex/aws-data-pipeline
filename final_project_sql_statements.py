class SqlQueries:
    
    songplay_table_insert = ("""
        BEGIN;
        DROP TABLE IF EXISTS songplays;
        CREATE TABLE songplays AS
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)


    dim_table_truncate = ("""
        TRUNCATE {};
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS {} (
            userid INT NOT NULL, 
            firstname TEXT,
            lastname TEXT,
            gender TEXT,
            level TEXT)
    """)

    user_table_insert = ("""
        INSERT INTO {} (userid, firstname, lastname, gender, level)
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong';
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS {} (song_id TEXT NOT NULL, title TEXT, artist_id TEXT, 
        year INT, duration FLOAT);""")

    song_table_insert = ("""
        INSERT INTO {} (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration 
        FROM staging_songs;
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS {}
        (artist_id TEXT NOT NULL, 
        artist_name TEXT, 
        artist_location TEXT, 
        artist_latitude FLOAT, 
        artist_longitude FLOAT);
    """)

    artist_table_insert = ("""
        INSERT INTO {} (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
    """)


    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS {} (start_time timestamp NOT NULL, "hour" int4, 
        "day" int4, week int4, "month" varchar(256), "year" int4, weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time));
    """)

    time_table_insert = ("""
        INSERT INTO {} 
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)


    count_nulls = ("""
        SELECT COUNT(*) AS null_count 
        FROM {} 
        WHERE {} IS NULL;""")

    count_rows = ("""SELECT COUNT(*) AS row_count FROM {};""")


    stage_events = """
    (artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration DOUBLE PRECISION,
    sessionId INT,
    song TEXT,
    status INT,
    ts bigint,
    userAgent TEXT,
    userId INT);"""

    stage_songs = """
    (song_id TEXT,
    num_songs INT,
    title TEXT,
    artist_name TEXT,
    artist_latitude FLOAT,
    year INT,
    duration FLOAT,
    artist_id TEXT,
    artist_longitude FLOAT,
    artist_location TEXT);"""
