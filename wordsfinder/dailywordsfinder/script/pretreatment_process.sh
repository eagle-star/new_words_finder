#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`
source $DIR/../conf/conf.sh

# 每天跟新的增量数据（输入）
PRIMARY_HDFS_PATH=${PROJECT_PATH}/primarydata
# 处理后增量数据（输出）
PROCESSED_NEW_DATA_PATH=${PROJECT_PATH}/feature/newdata/${1}
# 合并文件路劲（输入）
NEW_AND_OLD_DATA_PATH=${PROJECT_PATH}/feature/*ata/n_gram_
# 合并数据输出路径（输出）
MERGE_DATA_PATH=${PROJECT_PATH}/feature/tmp/n_gram_
# 每日统计原始数据
STATISTIC_DATA_PATH=${PROJECT_PATH}/feature/data

YETERDAY=`date -d "1 days ago" "+%y%m%d"`

# 确认昨天的抓取数据正常存储到hdfs中
if [ `hadoop fs -ls $PRIMARY_HDFS_PATH | awk 'NF==8{print $8}' |  wc -l` -eq 0 ]; then
    echo "do not find yesterday data, please chech update hdfs process in 10.10.4.41!"
    exit 1
fi

for i in {1..2}
do
    echo "loop $i"
    # 切词-统计
    hadoop fs -rm -r ${PROCESSED_NEW_DATA_PATH}*
    hadoop fs -rm -r ${MERGE_DATA_PATH}*

    #sudo sh /usr/local/spark-1.4.1/sbin/stop-all.sh
    #sleep 1m

    #echo "start spark, wait one minite!"
    #sudo sh /usr/local/spark-1.4.1/sbin/start-all.sh
    #sleep 1m

    spark-submit --num-executors 6 --executor-memory 4G --executor-cores 6 --conf spark.default.parallelism=500 --conf spark.storage.memoryFraction=0.5 --conf spark.shuffle.memoryFraction=0.3  $DIR/../python/pretreateddata.py $PRIMARY_HDFS_PATH/$1 ${PROCESSED_NEW_DATA_PATH}_tmp/n_gram_

    if [ $? -eq 0 ]; then
        # 替换汇总数据
        hadoop fs -rm -r ${PROCESSED_NEW_DATA_PATH}
        hadoop fs -mv  ${PROCESSED_NEW_DATA_PATH}_tmp ${PROCESSED_NEW_DATA_PATH}
        echo "$1 pretreatment_process is successful!"
        break
    else
        echo "stop spark, wait one minite!"
        #sudo sh /usr/local/spark-1.4.1/sbin/stop-all.sh
        #sleep 2m

        echo "start spark, wait one minite!"
        #sudo sh /usr/local/spark-1.4.1/sbin/start-all.sh
        #sleep 2m
        if [ $i -eq 3 ]; then
            echo "pretreatment_process is failed!"
            exit 1
        fi
    fi
done



