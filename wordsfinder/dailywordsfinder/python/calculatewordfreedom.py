# _*_ coding:utf-8 _*_
"""
   计算自由度
    @author kaolafm
    @create 2018-06-20 16:54
"""
from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import math

def parseAndStatistic(s):
    """
    前词或后词统计
    :param s:
    :return:
    """
    sum1 = 0
    worddict = {}

    it = s.split(";")
    for info in it:
        word_num = info.split(":")
        if len(word_num) != 2:
            continue

        word = word_num[0].strip()
        num = word_num[1]
        if cmp(word, "HEAD") != 0 and cmp(word, "END") != 0:
            # 求和
            sum1 += int(num)
            worddict[word] = num

    return sum1, worddict

def entropy(probability):
    """
    熵
    :param probability:概率
    :return: 熵
    """
    return - probability * math.log(probability, 2)

def calculateFreedom(s):
    """
    自由度
    :param s:
    :return:
    """
    sum1, worddict = parseAndStatistic(s)
    freedom = 0.0

    if sum1 == 0: # 检验一下
        freedom = sys.float_info.max
    else:
        for key, value in worddict.items(): # 过滤head or end
            probability = float(value) / float(sum1)
            freedom += entropy(probability)

    return freedom

def calculateProcess(inputpath, outputpath, nums):
    for n in range(1,6,1):
        calculateRDD = sc.textFile(inputpath + str(n))
        processedCalculateRDD = calculateRDD.map(lambda s : s.split("\t")).map(lambda s : (s[0], s[1], float(s[1])/float(nums[n-1]), min(calculateFreedom(s[2]), calculateFreedom(s[3]))))\
	    .map(lambda (word, num, probability, entropy) : "\t".join([word, str(num), str(probability), str(entropy)]))
        processedCalculateRDD.repartition(4).saveAsTextFile(outputpath + str(n))

if __name__ == '__main__':

    if len(sys.argv) != 8:
        print u'calculatewordfreedom : 输入参数个数错误！'
        exit(1)

    nums = []
    for i in  range(3, 8, 1):
        nums.append(sys.argv[i])

    sc = SparkContext(appName="calculatewordfreedom")
    calculateProcess(sys.argv[1], sys.argv[2], nums)
