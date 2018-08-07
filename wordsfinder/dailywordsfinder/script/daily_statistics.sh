#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`
source $DIR/../conf/conf.sh

YESTERDAY=`date -d "1 days ago" "+%Y-%m-%d"`

#每天清理一下之前的数据
hadoop fs -rm -r $PROJECT_PATH/daily

#计算
spark-submit $DIR/../python/words_analyzer.py $PROJECT_PATH/primarydata $PROJECT_PATH/daily $WORDSFINDER/dict/pos_stop.dict

#download from hdfs
hadoop fs -cat $PROJECT_PATH/daily/* > $LOCAL_PROJECT_PATH/data/primary_daily_words_$YESTERDAY

#第一级过滤 自由度/凝固度等过滤
python $DIR/../python/local_filter_by_feature.py $LOCAL_PROJECT_PATH/data/primary_daily_words_$YESTERDAY  $LOCAL_PROJECT_PATH/conf/params $LOCAL_PROJECT_PATH/data/sorted_filtered_words 

# cat到同一个文件下
cat $LOCAL_PROJECT_PATH/data/sorted_filtered_words_* > $LOCAL_PROJECT_PATH/data/middle_sorted_filtered_words_1

#停词/数字/字母等过滤
python $DIR/../python/general_filter.py $LOCAL_PROJECT_PATH/data/middle_sorted_filtered_words_1 $WORDSFINDER/dict/pos_stop.dict $WORDSFINDER/dict/neg_dict > $LOCAL_PROJECT_PATH/data/middle_sorted_filtered_words


# 统计-总词频 文章数 反tf-idf
python $DIR/../python/cal_tf_idf.py $WORDSFINDER/dict/addingwords/new_words_${YESTERDAY} $LOCAL_PROJECT_PATH/dict/newwords/daily_new_words_${YESTERDAY} $LOCAL_PROJECT_PATH/data/middle_sorted_filtered_words $LOCAL_PROJECT_PATH/data/primary_daily_words_$YESTERDAY $WORDSFINDER/dict/words_daily

#发现新词补充追加到词典后面 
cat $LOCAL_PROJECT_PATH/dict/newwords/daily_new_words_${YESTERDAY} | awk '{print $1}' >> $WORDSFINDER/dict/words_daily

# 发邮件
sh $DIR/emailwords.sh
