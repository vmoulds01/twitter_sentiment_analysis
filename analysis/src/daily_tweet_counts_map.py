from pyspark.sql.functions import date_format
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf
from plotly.graph_objs import Scatter, Layout, Choropleth, Frame
import plotly

###################################################################################################################################################################
#
#               PLOT CHOROPLETH MAP SHOWING VARIATION IN TWEET COUNTS BY COUNTRY OVER TIME
#
###################################################################################################################################################################


def create_spark_session():
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local[*]") \
        .appName("sentiment_analysis") \
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def plot(plot_dic, height=800, width=800, **kwargs):
    kwargs['output_type'] = 'div'
    plot_str = plotly.offline.plot(plot_dic, **kwargs)
    print('%%angular <div style="height: %ipx; width: %spx"> %s </div>' %
          (height, width, plot_str))


spark_session = create_spark_session()
data = spark_session.sql(
    "select datestring, countryterritorycode,  sum(tweetCounts) as tweets from dailyCountryCovidSentiment group by countryterritorycode, datestring")
data_df = data.toPandas()
data_df = data_df.set_index(['datestring'])


fig_dict = {
    "data": [],
    "layout": {},
    "frames": []
}

# fill in most of layout
fig_dict["layout"]["title"] = "Tweet counts over March 2020"
fig_dict["layout"]["updatemenus"] = [
    {
        "buttons": [
            {
                "args": [None, {"frame": {"duration": 500, "redraw": True},
                                "fromcurrent": True, "transition": {"duration": 300,
                                                                    "easing": "quadratic-in-out"}}],
                "label": "Play",
                "method": "animate"
            },
            {
                "args": [[None], {"frame": {"duration": 0, "redraw": True},
                                  "mode": "immediate",
                                  "transition": {"duration": 0}}],
                "label": "Pause",
                "method": "animate"
            }
        ],
        "direction": "left",
        "pad": {"r": 10, "t": 87},
        "showactive": False,
        "type": "buttons",
        "x": 0.1,
        "xanchor": "right",
        "y": 0,
        "yanchor": "top"
    }
]

sliders_dict = {
    "active": 0,
    "yanchor": "top",
    "xanchor": "left",
    "currentvalue": {
        "font": {"size": 20},
        "prefix": "Date:",
        "visible": True,
        "xanchor": "right"
    },
    "transition": {"duration": 300, "easing": "cubic-in-out"},
    "pad": {"b": 10, "t": 50},
    "len": 0.9,
    "x": 0.1,
    "y": 0,
    "steps": []
}

# make data
fig_dict["data"].append(Choropleth(locations=data_df["countryterritorycode"].loc["01/03/2020"],  # Spatial coordinates
                                   z=data_df["tweets"].loc["01/03/2020"]))

# make frames
for idx in sorted(data_df.index.unique()):
    frame = {"data":  [Choropleth(locations=data_df["countryterritorycode"].loc[idx],
                                  z=data_df["tweets"].loc[idx])], "name": str(idx)}

    fig_dict["frames"].append(frame)
    slider_step = {"args": [
        [idx],
        {"frame": {"duration": 300, "redraw": True},
         "mode": "immediate",
         "transition": {"duration": 300}}
    ],
        "label": idx,
        "method": "animate"}
    sliders_dict["steps"].append(slider_step)


fig_dict["layout"]["sliders"] = [sliders_dict]


plot(fig_dict)
