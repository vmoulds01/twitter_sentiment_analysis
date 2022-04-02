from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DateType, IntegerType, StringType, StructField, StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, concat, count, date_format, isnull, lit, split, to_date, udf, when
from twarc import Twarc

TWITTER_KEY = ''
TWITTER_KEY_SECRET = ''
TWITTER_ACCESS_TOKEN = ''
TWITTER_ACCESS_TOKEN_SECRET = ''

def create_spark_session():
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local[*]") \
        .appName("sentiment_analysis") \
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def setup_twarc():
    consumer_key = TWITTER_KEY
    consumer_secret = TWITTER_KEY_SECRET
    access_token = TWITTER_ACCESS_TOKEN
    access_token_secret = TWITTER_ACCESS_TOKEN_SECRET

    t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
    return t

def hydrate_tweets_and_save_to_hive():
    spark_session = create_spark_session()
    tweet_df = spark_session.sql(
        "select t1.Tweet_ID from sentimentSummary t1 left join hydratedTweets t2 on t2.Tweet_ID = t1.Tweet_ID where t1.country in ('GB', 'UK', 'IE') and t2.Tweet_ID IS NULL")
    hydrated_df = hydrate_tweet_df(spark_session, tweet_df)
    hydrated_df.registerTempTable("temptable")
    spark_session.sql("insert into table hydratedTweets select * from temptable")
    

def hydrate_tweet_df(spark, twitter_df):
    t = setup_twarc()
    twitter_ids = twitter_df.select(
        'Tweet_ID').rdd.flatMap(lambda x: x).top(10000)
    cols = ['Tweet_ID', 'full_text']
    vals = []
    for tweet in t.hydrate(twitter_ids):
        vals.append((tweet['id'], tweet['full_text']))
    hdyrated_df = spark.createDataFrame(vals, cols)
    return hdyrated_df

if __name__ == '__main__':
    hydrate_tweets_and_save_to_hive()
