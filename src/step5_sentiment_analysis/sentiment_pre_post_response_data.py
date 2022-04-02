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


def create_pre_post_sentiment_resopnse_metrics(spark, sentiment_df, response_df):
    SeriesAppend = []
    distinct_countries = response_df.select(
        'Country').distinct().rdd.flatMap(lambda x: x).top(200)
    for country_value in distinct_countries:
        country_sentiment_df = sentiment_df.filter(
            sentiment_df.Country.isin(country_value))
        filter_date = response_df.filter(response_df.Country.isin(country_value)).select(
            'StartDate').rdd.flatMap(lambda x: x).top(1)[0]
        pre_sentiment = country_sentiment_df.filter(
            country_sentiment_df.daterep < filter_date).agg({"avgSentiment": "avg"}).rdd.flatMap(lambda x: x).top(1)[0]
        post_sentiment = country_sentiment_df.filter(
            country_sentiment_df.daterep > filter_date).agg({"avgSentiment": "avg"}).rdd.flatMap(lambda x: x).top(1)[0]
        difference = post_sentiment - pre_sentiment
        SeriesAppend.append(
            (country_value, pre_sentiment, post_sentiment, difference))
    response_sentiment_diff_df = spark.createDataFrame(
        SeriesAppend, ['country', 'pre_sentiment', 'post_sentiment', 'diff_sentiment'])
    response_sentiment_diff_df.createOrReplaceTempView("response_diff_df_table")
    spark.sql(
        "create table if not exists responseSentimentDifference select * from response_diff_df_table")


if __name__ == '__main__':
    spark = create_spark_session()
    sentiment_df = spark.sql(
        "select t1.daterep, t1.Country, t1.countriesAndTerritories, t1.avgSentiment from dailyCountryCovidSentiment t1 where t1.countriesAndTerritories in (select t2.Country from covidResponse t2)")
    response_df = spark.sql(
        "select t2.Country, t1.StartDate from covidResponse t1 join dailyCountryCovidSentiment t2 on t2.countriesAndTerritories = t1.Country where t1.StartDate < '2020-04-01' and t1.EndDate > '2020-04-01' and t1.Response_measure in ('ClosPrim') ")
    create_pre_post_sentiment_resopnse_metrics(
        spark, sentiment_df, response_df)
