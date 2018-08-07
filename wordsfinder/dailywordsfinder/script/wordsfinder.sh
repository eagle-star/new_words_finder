#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`

date=`date -d "0 days ago" "+%y-%m-%d"`

sh $DIR/pretreatment_process.sh $1

if [ $? -eq 0 ]; then
    sh $DIR/statistics_process.sh $1
    if [ $? -eq 0 ]; then
	# 根据阈值获取字典
	#sh $DIR/get_all_words.sh
	echo $date " wordsfinder is successful!"
    fi
else
    echo "pretreatment_process is failed!"
    exit 1
fi

