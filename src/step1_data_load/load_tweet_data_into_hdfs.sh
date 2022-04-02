#!/usr/bin/env bash

# Upload summary data to hdfs
hdfs dfs -mkdir user
hdfs dfs -mkdir user/hadoop
hdfs dfs -mkdir user/hadoop/tweet_data
hdfs dfs -moveFromLocal /data_input/summary/ user/hadoop/tweet_data/summary


## Upload sentiment data to hdfs
hdfs dfs -moveFromLocal /data_input/sentiment/ user/hadoop/tweet_data/sentiment
