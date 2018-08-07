#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`
source $DIR/../conf/conf.sh
cat $DIR/../data/topics | while read topic
do
    sh $DIR/wordsfinder.sh $topic
    echo "$topic calculating is finished!"
done

echo "all topic is finished!"



