###################################################################################################################################################################
#
#               SCATTER PLOTS SHOWING VARIATION IN SENTIMENT WITH RELATION TO COVID CASES PER COUNTRY
#
###################################################################################################################################################################

import plotly
from plotly.graph_objs import Scatter, Layout, Choropleth, Frame
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import date_format


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
    "select datestring, countryterritorycode,  avg(avgsentiment) as sentiment, sum(cases) as dailycases from dailyCountryCovidSentiment group by countryterritorycode, datestring")
data_df = data.toPandas()
data_df = data_df.set_index(['countryterritorycode'])
data_df.sort_values(by=['dailycases'], inplace=True)


fig_dict = {
    "data": [],
    "layout": {},
    "frames": []
}

# fill in most of layout
fig_dict["layout"]["title"] = "Sentiment versus number of covid cases by country"
fig_dict["layout"]["updatemenus"] = [
    {
        "buttons": [
            {
                "args": [None, {"frame": {"duration": 500, "redraw": True},
                                "fromcurrent": False, "transition": {"duration": 300,
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
        "prefix": "Country code:",
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
fig_dict["data"].append(Scatter(x=data_df["sentiment"].loc["AUS"],  # Spatial coordinates
                                y=data_df["dailycases"].loc["AUS"], mode="markers"))

# make frames
for idx in sorted(data_df.index.unique()):
    if data_df.loc[idx].shape[0] > 7:
        frame = {"data":  [Scatter(x=data_df["sentiment"].loc[idx],
                                   y=data_df["dailycases"].loc[idx], mode="markers")], "name": str(idx)}

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
