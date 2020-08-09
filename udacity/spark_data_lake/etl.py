import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, DateType


def create_spark_session():
    """
    Creates a spark session instance to be called by queries.
    
    Returns
    -------
    spark : pyspark.sql.SparkSession
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song_data json files according to expected schema and creates songs and artists tables.
    
    Parameters
    ----------
    spark : pyspark.sql.SparkSession
    input_data : S3 bucket containing input json files
    output_data : S3 bucket where parquet files for fact and dimension tables will be written
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # define schema for songs data as some fields were parsed with wrong type
    song_schema = StructType([
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_name", StringType()),
            StructField("duration", DoubleType()),
            StructField("num_songs", IntegerType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ])
    
    # read song data file
    dfSongs = spark.read.json(song_data, schema=song_schema)
    dfSongs.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration
        FROM song_data
        WHERE song_id IS NOT NULL
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_path = output_data + 'songs'
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_path)

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, 
                    artist_name AS name, 
                    artist_location AS location, 
                    artist_latitude AS latitude, 
                    artist_longitude AS longitude
        FROM song_data
        WHERE artist_id IS NOT NULL
        """)
    
    # write artists table to parquet files
    artists_path = output_data + 'artists'
    artists_table.write.parquet(artists_path)


def process_log_data(spark, input_data, output_data):
    """
    Reads log_data json files according to expected schema and creates users, time and songplays tables.
    
    Parameters
    ----------
    spark : pyspark.sql.SparkSession
    input_data : S3 bucket containing input json files
    output_data : S3 bucket where parquet files for fact and dimension tables will be written
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file and create temp view
    dfLog = spark.read.json(log_data)
    dfLog.createOrReplaceTempView("log_data")
    
    # filter by actions for song plays and recreate temp view
    dfLog = spark.sql("""
        SELECT *
        FROM log_data
        WHERE page = 'NextSong'
        """)
    dfLog.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userId as user_id, 
                    firstName as first_name, 
                    lastName as last_name, 
                    gender, 
                    level
        FROM log_data
        WHERE userId IS NOT NULL
        """) 
    
    # write users table to parquet files
    users_path = output_data + 'users'
    users_table.write.parquet(users_path)

    # create timestamp column from original timestamp column and recreate temp view
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0), TimestampType()) 
    dfLog = dfLog.withColumn("start_time", get_timestamp(dfLog.ts))
    dfLog.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT start_time, hour(start_time) AS hour, 
                    day(start_time) AS day, 
                    extract(week from start_time) AS week, 
                    month(start_time) AS month, 
                    year(start_time) AS year, 
                    date_format(start_time, 'EEEE') AS weekday
        FROM log_data
        """)
    
    # write time table to parquet files partitioned by year and month
    time_path = output_data + 'time'
    time_table.write.partitionBy('year', 'month').parquet(time_path)

    # read in song data to use for songplays table
    # ignoring specific schema as the columns with wrong format are not used here
    song_data = input_data + 'song_data/*/*/*/*.json'
    dfSongs = spark.read.json(song_data)
    dfSongs.createOrReplaceTempView("song_data") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT DISTINCT l.start_time, 
               l.userId AS user_id, 
               l.level, 
               s.song_id, 
               s.artist_id, 
               l.sessionId AS session_id, 
               l.location, 
               l.userAgent AS user_agent,
               year(l.start_time) AS year,
               month(l.start_time) AS month
        FROM log_data l
        JOIN song_data s ON l.artist = s.artist_id AND l.song = s.title
        """) 
    
    # adding songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_path = output_data + 'songplays'
    songplays_table.write.partitionBy('year', 'month').parquet(songplays_path)


def main():
    """
    1. Initializes a Spark session 
    2. Triggers processes that read log_data and song_data json files
       and writes analytics-modelled tables to parquet files on s3.
    """
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3://dl-sparkify/"   
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
