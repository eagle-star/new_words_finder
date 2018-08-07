#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`

hadoop fs -rm /user/recomm/wordsfinder/data/primarydata/ttt*
hadoop fs -put $1 /user/recomm/wordsfinder/data/primarydata/

sh $DIR/wordsfinder.sh

echo $1 " is finished!"
