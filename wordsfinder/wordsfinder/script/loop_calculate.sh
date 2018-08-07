#!/bin/sh

source ~/.bash_profile

DIR=`dirname $0`

ls -l /home/recomm/test_zhoujx/wordsfinder/data/seperatedata/* | awk 'NF==9 {print $9}' >  $DIR/../data/loopfile
cat $DIR/../data/loopfile | while read line
do
    sh $DIR/do_one_file.sh $line
done


