import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                event_id    BIGINT IDENTITY(0,1) NOT NULL,
                artist      TEXT,
                auth        TEXT,
                firstName   TEXT,
                gender      TEXT,
                itemInSession TEXT ,
                lastName    TEXT,
                length      TEXT,
                level       TEXT,
                location    TEXT,
                method      TEXT,
                page        TEXT,
                registration TEXT,
                sessionId   INTEGER NOT NULL SORTKEY DISTKEY,
                song        TEXT,
                status      INTEGER,
                ts          BIGINT NOT NULL,
                userAgent   TEXT,
                userId      INTEGER                  );""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER ,
                artist_id           TEXT NOT NULL SORTKEY DISTKEY,
                artist_latitude     TEXT ,
                artist_longitude    TEXT ,
                artist_location     TEXT ,
                artist_name         TEXT ,
                song_id             TEXT NOT NULL,
                title               TEXT ,
                duration            DECIMAL(9) ,
                year                INTEGER          );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                songplay_id INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
                start_time  TIMESTAMP NOT NULL,
                user_id     TEXT NOT NULL DISTKEY,
                level       TEXT NOT NULL,
                song_id     TEXT NOT NULL,
                artist_id   TEXT NOT NULL,
                session_id  TEXT NOT NULL,
                location    TEXT ,
                user_agent  TEXT             
    );""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS user_table (
                user_id     INTEGER NOT NULL SORTKEY,
                first_name  TEXT,
                last_name   TEXT,
                gender      TEXT,
                level       TEXT              
    ) diststyle all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
                song_id     TEXT NOT NULL SORTKEY,
                title       TEXT NOT NULL,
                artist_id   TEXT NOT NULL,
                year        INTEGER NOT NULL,
                duration    DECIMAL(9) NOT NULL
    );""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                artist_id   TEXT NOT NULL SORTKEY,
                name        TEXT,
                location    TEXT,
                latitude    DECIMAL(9),
                longitude   DECIMAL(9)               
    ) diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP NOT NULL SORTKEY,
                hour        SMALLINT,
                day         SMALLINT,
                week        SMALLINT,
                month       SMALLINT,
                year        SMALLINT,
                weekday     SMALLINT                 
    ) diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent) 
                            SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
                                    se.userId                   AS user_id,
                                    se.level                    AS level,
                                    ss.song_id                  AS song_id,
                                    ss.artist_id                AS artist_id,
                                    se.sessionId                AS session_id,
                                    se.location                 AS location,
                                    se.userAgent                AS user_agent
                            FROM staging_events AS se JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
                            WHERE se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO user_table (user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                        SELECT  DISTINCT se.userId          AS user_id,
                                se.firstName                AS first_name,
                                se.lastName                 AS last_name,
                                se.gender                   AS gender,
                                se.level                    AS level
                        FROM staging_events AS se
                        WHERE se.page = 'NextSong';""")

song_table_insert = ("""INSERT INTO song(song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
                        SELECT  DISTINCT ss.song_id         AS song_id,
                                ss.title                    AS title,
                                ss.artist_id                AS artist_id,
                                ss.year                     AS year,
                                ss.duration                 AS duration
                        FROM staging_songs AS ss;""")

artist_table_insert = ("""INSERT INTO artist (artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
                            SELECT  DISTINCT ss.artist_id       AS artist_id,
                                    ss.artist_name              AS name,
                                    ss.artist_location          AS location,
                                    ss.artist_latitude          AS latitude,
                                    ss.artist_longitude         AS longitude
                            FROM staging_songs AS ss;
""")

time_table_insert = ("""INSERT INTO time (start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
                        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                                    * INTERVAL '1 second'        AS start_time,
                                EXTRACT(hour FROM start_time)    AS hour,
                                EXTRACT(day FROM start_time)     AS day,
                                EXTRACT(week FROM start_time)    AS week,
                                EXTRACT(month FROM start_time)   AS month,
                                EXTRACT(year FROM start_time)    AS year,
                                EXTRACT(week FROM start_time)    AS weekday
                        FROM    staging_events                   AS se
                        WHERE se.page = 'NextSong';""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
