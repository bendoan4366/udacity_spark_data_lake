song_query = "SELECT DISTINCT song_id, title, artist_id, year, duration FROM songs"
artist_query ="SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs"
filter_songplays_query = "SELECT * FROM staging_events WHERE page = 'NextSong'"

user_query = """
            SELECT DISTINCT userId, firstName, lastName, gender, level
            FROM staging_events_filtered
            """

time_query = """
            SELECT DISTINCT timestamp as start_time, 
                            hour(timestamp) as hour,
                            day(timestamp) as day,
                            weekofyear(timestamp) as week,
                            month(timestamp) as month,
                            year(timestamp) as year,
                            weekday(timestamp) as weekday
            FROM (
                SELECT CAST(ts/1000 AS Timestamp) AS timestamp
                FROM staging_events_filtered
                )
            """

songplay_query = """
                SELECT month(CAST(events.ts/1000 AS Timestamp)) as month,
                       year(CAST(events.ts/1000 AS Timestamp)) as year,
                       CAST(events.ts/1000 AS Timestamp) AS timestamp,
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
