# _*_ coding:utf-8 _*_
"""
    数据前期处理 n-gram
    @author kaolafm
    @create 2018-06-12 20:02
"""

from pyspark import SparkConf, SparkContext, StorageLevel
import sys

def cutLine(inputdata, n):
    """
    n-gram 切词
    :param inputdata: 输入数据
    :param n:  n元
    :return:  切词list
    """

    wordslist = []
    lenStr = len(inputdata)

    for i in range(lenStr-n+1):
        if lenStr -i + 1 < 1:
            break
        head = "HEAD:1"
        end = "END:1"
        word = inputdata[i: i+n]
	
        if word.find(" ") == -1:
            if i >= 1 and lenStr -i > n:
                if cmp(inputdata[i-1:i], " ") != 0:
                    head = inputdata[i-1:i] + ":1"
                if cmp(inputdata[i+n:i+n+1], " ") != 0:
                    end = inputdata[i+n:i+n+1] + ":1"
            elif i == 0:
                if cmp(inputdata[i+n:i+n+1], " ") != 0:
                    end = inputdata[i+n:i+n+1] + ":1"
            else:
                if cmp(inputdata[i-1:i], " ") != 0:
                    head = inputdata[i-1:i] + ":1"
	    if n == 1:
		head = "HEAD:1"
        	end = "END:1"
            wordslist.append("_".join([word, head, end]))

    return wordslist

def mergefix(l):
    """
    统计前后缀频率
    :param l: 
    :return: 
    """
    fixdict = {}
    for fix in l:
        word_num = fix.split(":")
        if len(word_num) != 2:
            continue

        word = word_num[0]
        num = word_num[1]

        if fixdict.has_key(word):
            fixdict[word] = fixdict[word] + int(num)
        else:
            fixdict[word] = int(num)
    return fixdict

def convertfix(fixdict):
    """
    字典转化
    :param fixdict: 
    :return: 
    """
    l = []
    for key in fixdict:
        info = key + ":" + str(fixdict[key])
        l.append(info)

    return l

def newDataProcess(newDataInputpath, newDataOutput):
    """
    新增数据处理
    :param inputpath:
    :return:
    """
    testdata = sc.textFile(newDataInputpath)
    try:
        contentRDD = testdata.map(lambda s : s.split("\t")[1])
    except Exception, e:
        print Exception,":",e
    for n in range(1, 5, 1):
	# 数据切分/前后词等处理
        n_gram_result_map = contentRDD.flatMap(lambda lines : cutLine(lines, n)).map(lambda s : s.split("_"))\
            .map(lambda s : ((s[0]),([1,[s[1]],[s[2]]]))).reduceByKey(lambda num1,num2:((num1[0] + num2[0]), (num1[1] + num2[1]), (num1[2] + num2[2])))\
	    .map(lambda (word,(num, prefix, subfix)) : (word, num, convertfix(mergefix(prefix)), convertfix(mergefix(subfix))))
	# 一/二/三/四元存储 
	n_gram_result_merge = n_gram_result_map.map(lambda (word, num, prefix, subfix) : "\t".join([word, str(num), ";".join(prefix), ";".join(subfix)]))		

        n_gram_result_merge.repartition(1).saveAsTextFile(newDataOutput + str(n))

def oldAndNewDataUnion(oldDataInputpath, oldDataOutputpath):
    """
    新旧数据合并
    :param oldDataInputpath:
    :param newDataRDD:
    :return:
    """

    for n in range(1, 5, 1):
        #读取新旧文件 汇总特征
        mergedata = sc.textFile(oldDataInputpath + str(n)).map(lambda s : s.split("\t"))\
	    .map(lambda s : (s[0].strip(), str(s[1]).strip(), convertfix(mergefix(s[2].split(";"))), convertfix(mergefix(s[3].split(";")))))\
	    .map(lambda s : ((s[0]),(s[1],s[2],s[3]))).reduceByKey(lambda (num1, prefix1, subfix1),(num2, prefix2, subfix2): ((int(num1) + int(num2)), (prefix1 + prefix2), (subfix1 + subfix2)))\
	    .map(lambda (word, (num, prefix, subfix)) : (word, num, convertfix(mergefix(prefix)), convertfix(mergefix(subfix)))).filter(lambda (word, num, prefix, subfix) : int(num) > 1)
	#
        mergedata.map(lambda (word, num, prefix, subfix) : "\t".join([word, str(num), ";".join(prefix), ";".join(subfix)]))\
	    .repartition(4).saveAsTextFile(oldDataOutputpath + str(n))

if __name__ == '__main__':
    if len(sys.argv) != 5 :
        print u'pretreateddata : 输入参数个数错误！'
        exit(1)

    sc = SparkContext(appName="pretreateddata")

    newDataProcess(sys.argv[1], sys.argv[2])

    oldAndNewDataUnion(sys.argv[3], sys.argv[4])






