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

# 清空中间昨天的数据
rm $LOCAL_PROJECT_PATH/dict/middledict/*

if [ `ls -l primary_${1}_n_gram_* | wc -l ` -ge 4 ]; then
   python $DIR/../python/genwordsdict.py $LOCAL_PROJECT_PATH/dict/pos_stop.dict $LOCAL_PROJECT_PATH/data/primary_n_gram_ $LOCAL_PROJECT_PATH/dict/middledict/dict_n_gram_ $LOCAL_PROJECT_PATH/conf/params
fi

# 成词
MIDDLE_DICT_PATH=$LOCAL_PROJECT_PATH/dict/middledict
cat $MIDDLE_DICT_PATH/*pos > $LOCAL_PROJECT_PATH/dict/words_tmp  
# 不成词
cat $MIDDLE_DICT_PATH/*neg > $LOCAL_PROJECT_PATH/dict/not_words

# 与旧字典比较，获取新词
python $DIR/../python/check_new_words.py $LOCAL_PROJECT_PATH/dict/words_tmp $LOCAL_PROJECT_PATH/dict/words $LOCAL_PROJECT_PATH/dict/addingwords/new_words_$YESTERDAY

if [ $? -eq 0 ]; then
    rm $LOCAL_PROJECT_PATH/dict/words
    mv $LOCAL_PROJECT_PATH/dict/words_tmp $LOCAL_PROJECT_PATH/dict/words
fi

echo "get_all_words is successful!"

