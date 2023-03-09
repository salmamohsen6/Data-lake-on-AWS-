import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import  TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#global vraiables
input_data = "s3a://udacity-dend/"
output_data =  "s3a://jk-loaded-data/"



def process_song_data(spark: SparkSession)-> None:
    """
    Process song data and create the songs and artists tables.extracting colums for these tables .
    """
    # Get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # Read song data file
    df = spark.read.json(song_data)

    # Extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "year", "duration"
    ).dropDuplicates()

    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(
        os.path.join(output_data, "songs"), "overwrite"
    )

    # Extract columns to create artists table
    artists_table = df.select(
        "artist_id", "artist_name", "artist_location",
        "artist_latitude", "artist_longitude"
    ).dropDuplicates()

    # Rename columns in artists table
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                                 .withColumnRenamed("artist_location", "location") \
                                 .withColumnRenamed("artist_latitude", "latitude") \
                                 .withColumnRenamed("artist_longitude", "longitude")

    # Write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"), "overwrite")
    

def process_log_data(spark: SparkSession)-> None:
    """
    Process log data and create the users, time, and songplays tables extracting colums for thes tables .
    """
   
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = df.filter(df.page == 'NextSong') \
                   .select('ts', 'userId', 'level', 'song', 'artist',
                           'sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level').dropDuplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:str(int(x)/1000))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime =udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select('datetime') \
                           .withColumn( log_df.datetime.alias("start_time")) \
                           .withColumn(hour("timestamp").alias("hour")) \
                           .withColumn(dayofmonth("timestamp").alias("day")) \
                           .withColumn(weekofyear("timestamp").alias("week")) \
                           .withColumn(month("timestamp").alias("month")) \
                           .withColumn(year("timestamp").alias("year")) \
                           .withColumn(dayofweek('datetime').alias("weekday")) \
                           .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    try:
        time_table.write.partitionBy('year', 'month') \
                        .parquet(os.path.join(output_data,'/time.parquet'), 'overwrite')
    except Exception as e:
        print(f"Error writing time table to parquet files: {e}")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    
    log_df = log_df.alias('log_df')
    song_df = song_df.alias('song_df')
    
    joined_df = log_df.join(song_df, col('log_df.artist') == col(
        'song_df.artist_name'),how="inner")
    
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    
    # write songplays table to parquet files partitioned by year and month
    try:
        songplays_table.write.partitionBy('year','month')\
        .parquet(os.path.join(output_data, 'songplays.parquet'), mode='overwrite')
    except Exception as e:
        print(f"Error writing songplays table to parquet files: {e}")
        
def main():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    process_song_data(spark)    
    process_log_data(spark)


if __name__ == "__main__":
    main()
