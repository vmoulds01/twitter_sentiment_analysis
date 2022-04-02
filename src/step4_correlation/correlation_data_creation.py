from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DateType, IntegerType, StringType, StructField, StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, concat, count, date_format, isnull, lit, split, to_date, udf, when


def create_spark_session():
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local[*]") \
        .appName("correlation_analysis") \
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def create_correlation_metrics(spark, test_df):
    SeriesAppend = []
    distinct_countries = test_df.select(
        'Country').distinct().rdd.flatMap(lambda x: x).top(200)
    for country_value in distinct_countries:
        country_df = test_df.filter(test_df.Country.isin(country_value))
        cases_corr_results = country_df.stat.corr('tweetCounts', 'cases')
        deaths_corr_results = country_df.stat.corr('tweetCounts', 'deaths')
        cases_sentiment_corr = country_df.stat.corr('avgSentiment', 'cases')
        deaths_sentiment_corr = country_df.stat.corr('avgSentiment', 'deaths')
        SeriesAppend.append(
            (country_value, cases_corr_results, deaths_corr_results, cases_sentiment_corr, deaths_sentiment_corr))
    correlation_df = spark.createDataFrame(
        SeriesAppend, ['country', 'cases_tweet_correlation', 'deaths_tweet_correlation', 'cases_sentiment_corr', 'deaths_sentiment_corr'])
    correlation_df.createOrReplaceTempView("correlation_df_table")
    spark.sql(
        "create table if not exists correlationData select * from correlation_df_table")

if __name__ == '__main__':
    spark = create_spark_session()
    test_df = spark.sql(
        "select daterep, Country, tweetCounts, cases, deaths, avgSentiment from dailyCountryCovidSentiment where cases IS NOT NULL and deaths IS NOT NULL")
    create_correlation_metrics(spark, test_df)
