#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`

source $DIR/../conf/base.sh

DATE=`date -d "1 days ago" "+%Y-%m-%d"`

NEWWORDS=`cat $DIR/../dict/addingwords/new_words_${DATE} | awk 'NF==5 {print $1}' | xargs echo`
echo $NEWWORDS
send_mail "WORDS_FINDER" "newword_${NEWWORDS}" "guhb@kaolafm.com" "zhoujx@kaolafm.com"
