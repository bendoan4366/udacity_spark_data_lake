import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import song_query, artist_query, filter_songplays_query, user_query, time_query, songplay_query


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates spark session and configures session with hadoop and jar packages. Returns a spark object that can be used to run/execute spark queries
    """    

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    1. Loads data from song_data dataset and extracts columns for songs and artist tables 
    2. Writes tables to parquet files in s3.
    
    ------------------------------------------------------------
    
    Parameters
    
    - spark: Spark session created by invoking create_spark_session
    - input_data: Path to the song_data s3 bucket
    - output_data: Path to where the parquet files will be written in s3
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql(song_query)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql(artist_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    1. Loads data from log_data dataset and extracts columns for user, time, and songplays tables 
    2. Writes tables to parquet files in s3.
    
    ------------------------------------------------------------
    
    Parameters
    
    - spark: Spark session created by invoking create_spark_session
    - input_data: Path to the song_data s3 bucket
    - output_data: Path to where the parquet files will be written in s3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    df = spark.sql(filter_songplays_query)
    df.createOrReplaceTempView("staging_events_filtered")

    # extract columns for users table    
    users_table = spark.sql(user_query)
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")

    # extract columns to create time table
    time_table = spark.sql(time_query)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time.parquet", mode = "overwrite")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplay_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "/songplays/songplays.parquet", mode = "overwrite")


def main():
    """
    1. Creates spark session 
    2. Invokes process_song_data to read and write song and artist parquets
    3. Invokes process_log_data to read and write user, time, and songplay parquets
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://knd-udacity-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
