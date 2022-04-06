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
    "select countryterritorycode, sum(tweetCounts) as tweets from dailycountrycovidsentiment group by countryterritorycode")

plot_data = country_sentiment_data.toPandas()

plot({
    "data": [
        Choropleth(locations=plot_data['countryterritorycode'],
                   z=plot_data['tweets'])
    ],
    "layout": Layout(
        title="Tweets per country"
    )
})
