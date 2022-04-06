###################################################################################################################################################################
#
#               CALCULATE AVERAGE SENTIMENT & TWEET COUNTS
#
###################################################################################################################################################################
from pyspark.sql.functions import avg, sum, desc, asc
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf

def create_spark_session():
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local[*]") \
        .appName("sentiment_analysis") \
        .enableHiveSupport()\
        .getOrCreate()
    return spark


spark_session = create_spark_session()

country_sentiment_data = spark_session.sql(
    "select Country, avgSentiment, tweetCounts from dailyCountryCovidSentiment where tweetCounts > 100")
aggregated_df = country_sentiment_data.groupBy('Country').agg(avg('avgSentiment').alias(
    'sentiment'), sum('tweetCounts').alias('count')).sort(desc('sentiment'))

print(aggregated_df.head(5))
