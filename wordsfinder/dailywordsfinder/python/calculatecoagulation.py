# _*_ coding:utf-8 _*_
"""
    凝固度，产出最终数据形式
    @author kaolafm
    @create 2018-06-21 18:22
"""
import sys
import math
from pyspark import SparkConf, SparkContext, StorageLevel

def one_gram_dict(inputpath):
    """
    加载单字字典-概率
    :param inputpath:
    :return:
    """
    f = open(inputpath, 'r')
    line = f.readline()
    single_word_dict = {}
    print "start add single word in dict"
    while line:
        it = line.decode('utf-8').split("\t")
        if len(it) != 2:
	    line = f.readline()
            continue
        single_word = it[0].strip()
        try:
            probability = float(it[1])
        except Exception, e:
            probability = 0.0
        single_word_dict[single_word] = probability
        line = f.readline()
    f.close()
    return single_word_dict

def calculate_coagulation(s, union_probability, single_word_dict):
    """
    凝固度
    :param s:
    :return:
    """
    independent_probability = 1
    for i in range(len(s)):
        word = s[i:i+1]

	if single_word_dict.has_key(word):
            independent_probability *= single_word_dict[word]
	else:
	    independent_probability *= 1

    if independent_probability == 0:
	independent_probability = sys.float_info.min

    return math.log(float(union_probability) / independent_probability, 2)

def process_last_data(inputpath, outputpath, single_word_dict):
    """
    二/三/四元 凝固度
    :param inputpath:
    :param outputpath:
    :param single_word_dict:
    :return:
    """
    for i in range(2, 6, 1):
        dataRDD = sc.textFile(inputpath + str(i))
        wordfeatureRDD = dataRDD.map(lambda s : s.split("\t")).map(lambda (word, num, probability, entropy) : (word, num, probability, calculate_coagulation(word, probability, single_word_dict),entropy))\
            .map(lambda (word, num, probability, coagulation, freedom) : "\t".join([word, str(num), str(probability), str(coagulation), str(freedom)]))
        wordfeatureRDD.repartition(1).saveAsTextFile(outputpath + str(i))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print u'coagulationdegree : 输入参数个数错误！'
        exit(1)

    sc = SparkContext(appName="calculatecoagulationdegree")
    single_word_dict = one_gram_dict(sys.argv[1])
    broadcast_single_word_dict = sc.broadcast(single_word_dict) # broadcast single_word_dict
    process_last_data(sys.argv[2], sys.argv[3], broadcast_single_word_dict.value)
