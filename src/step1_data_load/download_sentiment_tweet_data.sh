#!/usr/bin/env bash

# Navigate to correct directory
if [ -d "/localDatasets/twitterData/sentiment_details" ]
then
  cd /localDatasets/twitterData/sentiment_details
else
  mkdir /localDatasets/twitterData/sentiment_details
  cd /localDatasets/twitterData/sentiment_details
fi

# Pull down summary data
counter_jan_day=22
counter_file=1
while [ $counter_jan_day -le 31 ]
do
  echo "$counter_jan_day"
  while [ $counter_file -le 9 ]
  do
    echo "$counter_file"
    wget https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset/master/Summary_Sentiment/2020_01/2020_01_"$counter_jan_day"_0"$counter_file"_Summary_Sentiment.csv
    ((counter_file++))
  done
  counter_file=1
  ((counter_jan_day++))
done

counter_jan_day=22
counter_file=10
while [ $counter_jan_day -le 31 ]
do
  echo "$counter_jan_day"
  while [ $counter_file -le 23 ]
  do
    echo "$counter_file"
    wget https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset/master/Summary_Sentiment/2020_01/2020_01_"$counter_jan_day"_"$counter_file"_Summary_Sentiment.csv
    ((counter_file++))
  done
  counter_file=10
  ((counter_jan_day++))
done
