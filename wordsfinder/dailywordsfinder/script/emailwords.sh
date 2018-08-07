#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`

source $DIR/../conf/base.sh

DATE=`date -d "1 days ago" "+%Y-%m-%d"`
TODAY=`date -d "0 days ago" "+%Y-%m-%d"`

NEWWORDS=`cat $DIR/../dict/newwords/daily_new_words_${DATE} | awk '{print $1}' | xargs echo`


#echo $NEWWORDS
#send_mail "WORDS_FINDER" "newword_${NEWWORDS}" "zhoujx@kaolafm.com"
send_mail "WORDS_FINDER_${TODAY}" "newword_${NEWWORDS}" "guhb@kaolafm.com" "zhoujx@kaolafm.com","liupc@kaolafm.com"
