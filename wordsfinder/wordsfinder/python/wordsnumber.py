# _*_ coding:utf-8 _*_
"""
   统计N元词总量
    @author kaolafm
    @create 2018-06-20 15:07
"""

from pyspark import SparkConf, SparkContext, StorageLevel
import sys

def statisticNum(name, inputpath):
    """
    n元统计总词数
    :param name:
    :param inputpath:
    :return:
    """
    NGramRDD = sc.textFile(inputpath).map(lambda s : s.split("\t")).map(lambda s : (name, s[1])).reduceByKey(lambda num1, num2 : (int(num1) + int(num2)))
    return NGramRDD

def ergodicNGram(pathIn, pathOut):
    """
    遍历n元
    :param pathIn:
    :param pathOut:
    :return:
    """
    for n in range(1, 5, 1):
        name = str(n) + '-gram'
        inputpath = pathIn + str(n)
        outputpath = pathOut + str(n)
        NGramRDD = statisticNum(name, inputpath).map(lambda (ngram, num) : "\t".join([ngram, str(num)]))
        NGramRDD.repartition(1).saveAsTextFile(outputpath)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print u'wordsnumber : 输入参数个数错误！'
        exit(1)

    sc = SparkContext(appName="statisticWordNum")

    ergodicNGram(sys.argv[1], sys.argv[2])


