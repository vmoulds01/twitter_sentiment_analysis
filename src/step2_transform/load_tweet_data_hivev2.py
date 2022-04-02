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

def transform_data():
    spark_session = create_spark_session()
    sentiment_summary_df = build_and_save_sentiment_summary_data(spark_session)
    hydrate_tweet_df(spark_session, sentiment_summary_df)
    retrieve_and_save_covid_response_data(spark_session)
    covid_19_df = retrieve_daily_covid_data(spark_session)
    daily_country_sentiment_df = build_country_sentiment_stats(
        spark_session, sentiment_summary_df)
    merge_and_save_covid_tweet_data(
        spark_session, daily_country_sentiment_df, covid_19_df)


def build_and_save_sentiment_summary_data(spark_session):
    summary_df = spark_session.read.csv('user/hadoop/tweet_data/summary/', header=True, inferSchema=True)
    sentiment_df = spark_session.read.csv(
        'user/hadoop/tweet_data/sentiment/', header=True, inferSchema=True)
    sentiment_summary_df = summary_df.join(sentiment_df, on=['Tweet_ID'], how='inner')\
        .withColumn("Tweet_Month", split(col("Date Created"), " ").getItem(1))\
        .withColumn("Tweet_Day", split(col("Date Created"), " ").getItem(2))\
        .withColumn("Tweet_Year", split(col("Date Created"), " ").getItem(5))\
        .withColumn("Tweet_Date", to_date(concat('Tweet_Day', 'Tweet_Month', 'Tweet_Year'), "ddMMMyyyy"))\
        .select('Tweet_ID', 'Country', 'Tweet_Date', 'Date Created', 'Sentiment_Label')\
        .filter(col('Country').isNotNull())\
        .drop_duplicates(subset=['Tweet_ID'])
    save_sentiment_summary_data_to_hive(
        spark_session, sentiment_summary_df)
    return sentiment_summary_df


def save_sentiment_summary_data_to_hive(spark_session, summary_df):
    summary_df.createOrReplaceTempView("sentiment_summary_df_table")
    spark_session.sql("create table if not exists sentimentSummary select * from sentiment_summary_df_table")


def build_country_sentiment_stats(spark_session, sentiment_summary_df):
    country_date_sentiment_stats = sentiment_summary_df.replace(to_replace='GB', value='UK', subset='Country').groupBy(
        "Country", "Tweet_Date").pivot("Sentiment_Label").agg(count("Sentiment_Label").alias("count")).na.fill(0)
    country_date_sentiment_stats_df = average_sentiment_calculation(country_date_sentiment_stats)
    return country_date_sentiment_stats_df


def average_sentiment_calculation(input_df):
    if not 'negative' in input_df.columns:
        input_df = input_df.withColumn('negative', lit(0))
    if not 'neutral' in input_df.columns:
        input_df = input_df.withColumn('neutral', lit(0))
    if not 'positive' in input_df.columns:
        input_df = input_df.withColumn('positive', lit(0))
    return input_df.withColumn('avgSentiment', (col('positive') - col('negative'))/(col('negative') + col('neutral') + col('positive')))\
                    .withColumn('tweetCounts', (col('negative') + col('neutral') + col('positive')))


def retrieve_daily_covid_data(spark_session):
    """ 
        Read in the daily covid 19 statistical data and transform date field before
        saving this data to a Hive table for use later.
    """
    daily_covid_data_df = spark_session.read.csv(
        'user/hadoop/covid_data/covid_daily_data_stats.csv', header=True, inferSchema=True)
    return daily_covid_data_df


def retrieve_and_save_covid_response_data(spark_session):
    input_df = spark_session.read.csv(
        'user/hadoop/covid_data/covid_response_data.csv', header=True, inferSchema=True)
    covid_response_data_df = input_df.withColumn("StartDate", to_date(input_df.date_start))\
        .withColumn("EndDate", to_date(input_df.date_end))\
        .replace(to_replace='United Kingdom', value='United_Kingdom', subset='Country')\
        .select('StartDate', 'EndDate', 'Country', 'Response_measure')
    covid_response_data_df.createOrReplaceTempView("covid_response_df_table")
    spark_session.sql(
        "create table if not exists covidResponse select * from covid_response_df_table")


def merge_and_save_covid_tweet_data(spark_session, daily_country_sentiment_df, covid_19_df):
    sentiment_df = daily_country_sentiment_df.select(date_format('Tweet_Date', 'dd/MM/yyyy').alias('dateString'),
                                                     'Country', 'tweetCounts', 'avgSentiment', 'negative', 'neutral', 'positive')
    joined_df = sentiment_df.join(covid_19_df, on=((covid_19_df.geoId == sentiment_df.Country) &
                                                   (sentiment_df.dateString == covid_19_df.dateRep)), how='left')
    joined_df.createOrReplaceTempView("merged_df_table")
    spark_session.sql(
        "create table if not exists dailyCountryCovidSentiment select * from merged_df_table")


def hydrate_tweet_df(spark, twitter_df):
    t = setup_twarc()
    twitter_ids = twitter_df.select(
        'Tweet_ID').rdd.flatMap(lambda x: x).top(100)
    cols = ['Tweet_ID', 'full_text']
    vals = []
    for tweet in t.hydrate(twitter_ids):
        vals.append((tweet['id'], tweet['full_text']))
    hdyrated_df = spark.createDataFrame(vals, cols)
    hdyrated_df.createOrReplaceTempView("hydrated_df_table")
    spark.sql(
        "create table if not exists hydratedTweets select * from hydrated_df_table")

if __name__ == '__main__':
    transform_data()
