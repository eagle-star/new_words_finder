# _*_ coding:utf-8 _*_
"""

    @author kaolafm
    @create 2018-06-28 17:15
"""

import numpy as np
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

def gen_stop_dict(inputpath):
    """

    :param inputpath:
    :return:
    """
    f = open(inputpath, 'r')
    line = f.readline()
    pos_stop_words_dict = {}
    while line:
        pos_stop_words_dict[line.strip()] = 1
        line = f.readline()

    return pos_stop_words_dict

def ischeckedword(word, pos_stop_words_dict):

    if u'的' not in word and u'也' not in word and u'了' not in word and u'是' not in word and u'和' not in word and u'在' not in word:
        return True

    if pos_stop_words_dict.has_key(word):
        return True
    else:
        return False

def get_freedom(freedom):

    if freedom < 1e300:
        return freedom
    else:
        return 0.0001

def filter_words(inputpath, freqthreshold, coagulationthreshold, freedomthreshold, pos_stop_words_dict):
    """
    过滤
    :param inputpath:
    :param outputpath:
    :param freqthreshold:
    :param coagulationthreshold:
    :param freedomthreshold:
    :param pos_stop_words_dict
    :param default:
    :return:
    """
    f = open(inputpath, 'r+')
    line = f.readline()

    pos_list = []
    neg_list = []
    while line:
        it = line.split("\t")
        if len(it) != 5:
            line = f.readline()
            continue
        freq_int = int(it[1].strip())
        coagulation_float = float(it[3].strip())
        freedom_float = get_freedom(float(it[4].strip()))
        if freq_int >= int(freqthreshold) and coagulation_float >= float(coagulationthreshold) and freedom_float >= float(freedomthreshold)\
                and ischeckedword(it[0].strip(), pos_stop_words_dict):
            pos_list.append(line)
        else:
            neg_list.append(line)
        line = f.readline()
    f.close()
    return pos_list, neg_list

def find_max_feature_value(inputpath):
    """
    最大值，用于归一化
    :param inputpath:
    :return:
    """
    f = open(inputpath, 'r+')
    line = f.readline()

    maxfreq = 0
    maxcoagulation = 0.0
    maxfreedom = 0.0

    while line:
        it = line.split("\t")
        if len(it) != 5:
            line = f.readline()
            continue

        freq = int(it[1].strip())
        coagulation = float(it[3].strip())
        freedom = float(it[4].strip())

        maxfreq = max(freq, maxfreq)
        maxcoagulation = max(coagulation, maxcoagulation)
        maxfreedom = max(freedom, maxfreedom)

        line = f.readline()

    return maxfreq, maxcoagulation, maxfreedom

def write_from_list(wordlist, outputpath):

    out = open(outputpath, 'a+')
    for line in wordlist:
        out.write(line)
    out.close()

def sortlist_by_freq(wordlist):
    return sorted(wordlist, key=lambda x : int(x.split("\t")[int(1)]), reverse=True)

def process_dic_gen(pos_stop_words_dict, inputpath, outputpath, freqthreshold, coagulationthreshold, freedomthreshold):
    # 过滤
    pos_list, neg_list = filter_words(inputpath, freqthreshold, coagulationthreshold, freedomthreshold, pos_stop_words_dict)
    # 排序（便于评估）
    pos_list_sorted = sortlist_by_freq(pos_list)
    neg_list_sorted = sortlist_by_freq(neg_list)
    # 输出到本地文件
    write_from_list(pos_list_sorted, outputpath + "_pos") # 文件是追加的
    write_from_list(neg_list_sorted, outputpath + "_neg")

def get_params(paramspath):
    """
    读取参数文件
    :param paramspath:
    :return:
    """
    f = open(paramspath)
    line = f.readline()
    params = []
    while line:
        it = line.split("|")
        params.append(it)
        line = f.readline()
    return params

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print u'genwordsdict 输入参数数量错误！'
        exit(1)
    # 生成停词正向词典
    pos_stop_words_dict = gen_stop_dict(sys.argv[1])
    params = get_params(sys.argv[4])
    for i in range(2, 5, 1):
        inputpath = sys.argv[2] + str(i)
        outputpath = sys.argv[3] + str(i)
        freqthreshold = int(params[i-2][0])
        coagulationthreshold = float(params[i-2][1])
        freedomthreshold = float(params[i-2][2])
        process_dic_gen(pos_stop_words_dict, inputpath, outputpath, freqthreshold, coagulationthreshold, freedomthreshold)
        print "words dict ", str(i), "-gram is successful!"

