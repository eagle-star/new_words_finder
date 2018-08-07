#!/bin/sh

source ~/.bash_profile
DIR=`dirname $0`
source $DIR/../conf/conf.sh
# 预处理数据位置
STATISTIC_DATA_PATH=${PROJECT_PATH}/feature/newdata/$1/n_gram_
# 统计词数
COUNT_PATH=${PROJECT_PATH}/feature/count/${1}_count
# 包括 概率和左右熵 的中间数据
LASTDATA_MID=${PROJECT_PATH}/feature/lastdata_tmp/$1/n_gram_
#
FEATURE_DATA_TMP=${PROJECT_PATH}/feature/feature_tem/$1/n_gram_
# 一元词本地位置
one_gram_probability=${LOCAL_PROJECT_PATH}/data/${1}

# 统计一~四元数据
hadoop fs -rm -r ${COUNT_PATH}_tmp/*
spark-submit $DIR/../python/wordsnumber.py $STATISTIC_DATA_PATH ${COUNT_PATH}_tmp/n_gram_
if [ $? -eq 0 ]; then
    hadoop fs -rm -r ${COUNT_PATH}
    hadoop fs -mv ${COUNT_PATH}_tmp $COUNT_PATH
    echo "count words nums successfully!"
else
    echo "count words nums failed!"
    exit 1
fi
# 四个元的总数
one_gram_num=`hadoop fs -cat ${COUNT_PATH}/n_gram_1/part-00000 | awk 'NF==2{print $2}'`
two_gram_num=`hadoop fs -cat ${COUNT_PATH}/n_gram_2/part-00000 | awk 'NF==2{print $2}'`
three_gram_num=`hadoop fs -cat ${COUNT_PATH}/n_gram_3/part-00000 | awk 'NF==2{print $2}'`
four_gram_num=`hadoop fs -cat ${COUNT_PATH}/n_gram_4/part-00000 | awk 'NF==2{print $2}'`
five_gram_num=`hadoop fs -cat ${COUNT_PATH}/n_gram_5/part-00000 | awk 'NF==2{print $2}'`

hadoop fs -rm -r ${LASTDATA_MID}*
spark-submit $DIR/../python/calculatewordfreedom.py $STATISTIC_DATA_PATH $LASTDATA_MID $one_gram_num $two_gram_num $three_gram_num $four_gram_num $five_gram_num

if [ $? -eq 0 ]; then
    hadoop fs -cat ${LASTDATA_MID}1/* | awk '{print $1 "\t" $3}' > ${one_gram_probability}_one_gram_probability
    hadoop fs -rm -r ${FEATURE_DATA_TMP}*
    spark-submit $DIR/../python/calculatecoagulation.py ${one_gram_probability}_one_gram_probability $LASTDATA_MID $FEATURE_DATA_TMP
    # 替换旧数据
    if [ $? -eq 0 ]; then
        echo "word features generate successfully!"
    else
        echo "word features generate failed!"
    fi
else
    echo "failed!"
fi
