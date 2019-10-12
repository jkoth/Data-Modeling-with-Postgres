# DROP TABLE STATEMENTS
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLE STATEMENTS
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS 
                                         songplays(songplay_id serial PRIMARY KEY, 
                                                   start_time bigint REFERENCES time(start_time), 
                                                   user_id integer NOT NULL REFERENCES users(user_id), 
                                                   level varchar, 
                                                   song_id char(19) NOT NULL REFERENCES songs(song_id), 
                                                   artist_id char(19) NOT NULL REFERENCES artists(artist_id), 
                                                   session_id integer NOT NULL, 
                                                   location varchar, 
                                                   user_agent text);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id integer PRIMARY KEY, 
                                                         first_name varchar, 
                                                         last_name varchar, 
                                                         gender char(1), 
                                                         level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id char(19) PRIMARY KEY, 
                                                         title text, 
                                                         artist_id char(19) REFERENCES artists(artist_id), 
                                                         year integer, 
                                                         duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id char(19) PRIMARY KEY, 
                                                             name varchar, 
                                                             location varchar, 
                                                             latitude varchar, 
                                                             longitude varchar);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time bigint PRIMARY KEY, 
                                                        hour integer, 
                                                        day integer, 
                                                        week integer, 
                                                        month integer, 
                                                        year integer, 
                                                        weekday integer);
""")

# INSERT STATEMENTS
songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT
                        DO NOTHING;
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT
                        DO NOTHING;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT
                        DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          VALUES(%s,%s,%s,%s,%s)
                          ON CONFLICT
                          DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT
                        DO NOTHING;
""")

# FIND SONG_ID AND ARTIST_ID FOR SONGPLAYS TABLE
song_select = ("""SELECT DISTINCT s.song_id, a.artist_id
                  FROM songs s
                  INNER JOIN artists a
                  ON (s.artist_id = a.artist_id)
                  WHERE s.title = %s
                    AND a.name = %s
                    AND s.duration = %s;
""")

# QUERY LISTS FOR EXECUTION
create_table_queries = [artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
