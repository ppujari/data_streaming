import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date
from pyspark.sql import SQLContext
from pyspark import SparkContext

"""https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html"""
"""Structured Streaming integration for Kafka 0.10 to poll data from Kafka."""

logger = logging.getLogger(__name__)

#Create a schema for incoming stream
 
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


@psf.udf(StringType())
def udf_convert_time(timestamp):
    d = parse_date(timestamp)
    return str(d.strftime('%y%m%d%H'))


def run_spark_job(spark):
    """
    Reading from Kafka. Spark Structured Streaming can read a stream from Kafka:
    Creating a Kafka Source Stream. We use Kafka connector. Construct a streaming DataFrame that reads from topic
    """
    # Create Spark Configuration
    # set up correct bootstrap server and port
    # Subscribe to the existing topic

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity-streaming-project") \
        .option("startingOffsets", "earliest") \
        .load()

    #df.isStreaming() # Returns True for DataFrames that have streaming sources

    df.printSchema()

    #Spark sees the data in binary, so convet to string.extract correct colums from kafka input stream 
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("service_call"))\
        .select("service_call.*")

    distinct_service_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # get different types of original_crime_type_name in 60 minutes interval
    counts_df = distinct_service_table \
        .withWatermark("call_datetime", "60 minutes") \
        .groupBy(
            psf.window(distinct_service_table.call_datetime, "10 minutes", "5 minutes"),
            distinct_service_table.original_crime_type_name
        ).count()

    #write streaming query on df to output stream
    query = counts_df \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    """
    Create Spark Session 
    """
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    logger.info("Spark Session started")

    run_spark_job(spark)

    spark.stop()


