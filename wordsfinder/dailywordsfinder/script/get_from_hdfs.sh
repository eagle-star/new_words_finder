#!/bin/sh

source ~/.bash_profile
DIR=`dirname $0`
source $DIR/../conf/conf.sh

YESTERDAY=`date -d "1 days ago" "+%Y-%m-%d"`

# 从hdfs上获取数据
for i in {2..5}
do
    hadoop fs -cat ${PROJECT_PATH}/feature/feature_tem/$1/n_gram_${i}/* >  $DIR/../data/primary_${1}_n_gram_${i}
done
