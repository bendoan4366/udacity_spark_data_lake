song_query = "SELECT DISTINCT song_id, title, artist_id, year, duration FROM songs LIMIT 200"
artist_query ="SELECT DISTINCT artist_id, artist_name, artist_location, aritst_latitude, artist_longitude"
filter_songplays_query = "SELECT * FROM staging_events WHERE page = 'NextSong'"

user_query = """
            SELECT DISTINCT userId, firstName, lastName, gender, level
            FROM staging_events_filtered
            """

time_query = """
            SELECT DISTINCT timestamp as start_time, 
                            hour(timestamp) as hour,
                            hour(timestamp) as day,
                            hour(timestamp) as week,
                            hour(timestamp) as month,
                            hour(timestamp) as year,
                            hour(timestamp) as weekday,
            FROM (
                SELECT cast (ts/1000 as Timestamp) as timestamp
                FROM staging_events_filtered
                )
            """

songplay_query = """
                SELECT events.timestamp as start_time, 
                        events.userId, 
                        events.level, 
                        events.sessionId, 
                        events.location, 
                        events.userAgent, 
                        songs.song_id, 
                        songs.artist_id
                FROM staging_events_filtered as events 
                JOIN songs ON events.song = songs.title
                """
