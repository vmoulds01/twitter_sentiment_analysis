###################################################################################################################################################################
#
#               PLOT CORRELATION STATS FOR CASES VS TWEET COUNTS PER COUNTRY
#
###################################################################################################################################################################

from pyspark import SparkConf
from pyspark.sql import SparkSession
import plotly
from plotly.graph_objs import Scatter, Layout, Choropleth, Frame, Bar


def plot(plot_dic, height=800, width=800, **kwargs):
    kwargs['output_type'] = 'div'
    plot_str = plotly.offline.plot(plot_dic, **kwargs)
    print('%%angular <div style="height: %ipx; width: %spx"> %s </div>' %
          (height, width, plot_str))


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
    "select t2.countryterritorycode, t1.cases_tweet_correlation from correlationdata t1, dailycountrycovidsentiment t2 where t2.country = t1.country")

plot_data = country_sentiment_data.toPandas()

plot({
    "data": [
        Choropleth(locations=plot_data['countryterritorycode'],
                   z=plot_data['cases_tweet_correlation'])
    ],
    "layout": Layout(
        title="Correlation between number of covid cases and tweet counts per country"
    )
})


###################################################################################################################################################################
#
#               PLOT CORRELATION STATS FOR DEATHS VS TWEET COUNTS PER COUNTRY
#
###################################################################################################################################################################



from pyspark import SparkConf
from pyspark.sql import SparkSession
import plotly
from plotly.graph_objs import Scatter, Layout, Choropleth, Frame, Bar


def plot(plot_dic, height=800, width=800, **kwargs):
    kwargs['output_type'] = 'div'
    plot_str = plotly.offline.plot(plot_dic, **kwargs)
    print('%%angular <div style="height: %ipx; width: %spx"> %s </div>' %
          (height, width, plot_str))


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
    "select t2.countryterritorycode, t1.deaths_tweet_correlation from correlationdata t1, dailycountrycovidsentiment t2 where t2.country = t1.country")

plot_data = country_sentiment_data.toPandas()

plot({
    "data": [
        Choropleth(locations=plot_data['countryterritorycode'],
                   z=plot_data['deaths_tweet_correlation'])
    ],
    "layout": Layout(
        title="Correlation between number of covid deaths and tweet counts per country"
    )
})

###################################################################################################################################################################
#
#               PLOT CORRELATION STATS FOR CASES VS SENTIMENT PER COUNTRY
#
###################################################################################################################################################################



def plot(plot_dic, height=800, width=800, **kwargs):
    kwargs['output_type'] = 'div'
    plot_str = plotly.offline.plot(plot_dic, **kwargs)
    print('%%angular <div style="height: %ipx; width: %spx"> %s </div>' %
          (height, width, plot_str))


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
    "select t2.countryterritorycode, t1.cases_sentiment_corr from correlationdata t1, dailycountrycovidsentiment t2 where t2.country = t1.country")

plot_data = country_sentiment_data.toPandas()

plot({
    "data": [
        Choropleth(locations=plot_data['countryterritorycode'],
                   z=plot_data['cases_sentiment_corr'])
    ],
    "layout": Layout(
        title="Correlation between number of covid cases and tweet sentiment per country"
    )
})


###################################################################################################################################################################
#
#               PLOT CORRELATION STATS FOR DEATHS VS SENTIMENT PER COUNTRY
#
###################################################################################################################################################################


def plot(plot_dic, height=800, width=800, **kwargs):
    kwargs['output_type'] = 'div'
    plot_str = plotly.offline.plot(plot_dic, **kwargs)
    print('%%angular <div style="height: %ipx; width: %spx"> %s </div>' %
          (height, width, plot_str))


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
    "select t2.countryterritorycode, t1.deaths_sentiment_corr from correlationdata t1, dailycountrycovidsentiment t2 where t2.country = t1.country")

plot_data = country_sentiment_data.toPandas()

plot({
    "data": [
        Choropleth(locations=plot_data['countryterritorycode'],
                   z=plot_data['deaths_sentiment_corr'])
    ],
    "layout": Layout(
        title="Correlation between number of covid deaths and tweet sentiment per country"
    )
})
