#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`
source $DIR/dailywordsfinder/conf/conf.sh

sh $WORDSFINDER/script/wordsfinder.sh > $WORDSFINDER/script/wordsfinder.log

sh $LOCAL_PROJECT_PATH/script/daily_statistics.sh > $LOCAL_PROJECT_PATH/script/daily_statistics.log

echo "wordsfinder is finished!"

