# _*_ coding:utf-8 _*_
"""
    
    @author kaolafm
    @create 2018-07-05 18:40
"""

from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import math

reload(sys)
sys.setdefaultencoding("utf-8")

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
    return wordslist, len(wordslist)

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

def num_and_char_fliter(s):

    if s.isdigit() or s.islower():
        return False
    else:
        return True

def date_filter(s):

    if s.find(u"年") == -1 and s.find(u"月") == -1 and s.find(u"日") == -1:
        return True

    if s.replace(u"年", "").isdigit() or s.replace(u"月", "").isdigit() or s.replace(u"日", "").isdigit():
        return False
    else:
        return True

def ischeckedword(word, pos_stop_words_dict):

    if u'的' not in word and u'也' not in word and u'了' not in word and u'是' not in word and u'和' not in word and u'在' not in word\
	  and num_and_char_fliter(word) and date_filter(word):
        return True

    if pos_stop_words_dict.has_key(word):
        return True
    else:
        return False

def filter_dict(wordsdict, n):
    """
    预过滤dict
    :param wordsdict: 待过滤字典
    :param n: 频率下限
    :return:过滤后字典
    """
    filtered_dict = {}
    for key in wordsdict:
        freq = int(wordsdict[key])
        if freq >= n:
            filtered_dict[key] = freq
    return filtered_dict

def mergefix(l, n):
    """
    统计前后缀频率
    :param l:
    :return:
    """
    word_freq_dict = {}
    word_prefix_dict = {}
    word_subfix_dict = {}
    for fix in l:
        word_info = fix.split("_")
        if len(word_info) != 3:
            continue

        word = word_info[0].strip()
        begin = word_info[1].strip()
        end = word_info[2].strip()

        # 统计词频
        if word_freq_dict.has_key(word):
            word_freq_dict[word] = word_freq_dict[word] + 1
            word_prefix_dict[word] = word_prefix_dict[word] + ";" + begin
            word_subfix_dict[word] = word_subfix_dict[word] + ";" + end

        else:
            word_freq_dict[word] = 1
            word_prefix_dict[word] = begin
            word_subfix_dict[word] = end
    # 过滤下词典 词频>1 低于1的可以不考虑
    filtered_word_freq_dict = filter_dict(word_freq_dict, n)
    filter_dict_tmp = {}
    for key in filtered_word_freq_dict:
        value = "_".join([str(filtered_word_freq_dict[key]), word_prefix_dict[key], word_subfix_dict[key]])
        filter_dict_tmp[key] = value

    return filter_dict_tmp

def calculate_one_gram_probability(filtered_dict, textlen):
    """
    文本内单字概率
    :param filtered_dict:
    :param textlen:
    :return:
    """
    one_gram_probability_dict = {}
    for key in filtered_dict:
        value = filtered_dict[key]
        one_gram_probability_dict[key] = float(value.split("_")[0]) / float(textlen)
    return one_gram_probability_dict

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
    return math.log(float(union_probability[s]) / independent_probability, 2)

def parseAndStatistic(s):
    """
    前词或后词统计
    :param s:
    :return:
    """
    sum1 = 0
    worddict = {}
    # 字符串分割 fix:num;fix:num
    wordlist = s.split(";")
    for fixs in wordlist:
        fix_num = fixs.split(":")
        fix = fix_num[0].strip()
        num = int(fix_num[1].strip())
        if cmp(fix, "HEAD") != 0 and cmp(fix, "END") != 0:
            # 求和
            sum1 += num
            if worddict.has_key(fix):
                worddict[fix] = worddict[fix] + num
            else:
                worddict[fix] = num
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

def freedom_dict_gen(worddict):
    """
    自由度计算
    :param worddict:
    :return:
    """
    freedom_dict = {}
    for key in worddict:
        key_info = worddict[key].split("_")
        freedom_dict[key] = min(calculateFreedom(key_info[1]), calculateFreedom(key_info[2]))
    return freedom_dict

def words_info_merge(filtered_dict, n_gram_probability_dict, coagulation_dict, freedom_dict):

    words_info = {}
    for key in filtered_dict:
        # if has_key
        info = "\t".join([str(filtered_dict[key].split("_")[0]), str(n_gram_probability_dict[key]), str(coagulation_dict[key]), str(freedom_dict[key])])
        words_info[key] = info
    sorted_words_info = sorted(words_info.items(), key= lambda x : x[1].split("\t")[0], reverse=True) # list
    return sorted_words_info

def final_filter(sorted_words_info, freqthresheld, coagulationthresheld, freedomthresheld, stopword_dict):

    filtered_sorted_words = []
    for word_info in sorted_words_info:
        infos = word_info[1].split("\t")
        freq = int(infos[0].strip())
        coagulation = float(infos[2].strip())
        freedom = float(infos[3].strip())
        if freq >= int(freqthresheld) and coagulation >= float(coagulationthresheld) and freedom >= float(freedomthresheld) and freedom <= 1.79769313486e300  and ischeckedword(word_info[0], stopword_dict):
	    fact = freq * coagulation * freedom
            filtered_sorted_words.append(word_info[0] + "_" + word_info[1].replace("\t", "_") + "_" + str(fact))
    return filtered_sorted_words

def calculate_processing(text, freqthresheld, coagulationthresheldt, freedomthresheld, stopword_dict):

    #一元参数获取
    one_gram_probability_dict = {}
    # all list
    all_word_list = []
    for i in range(1, 5, 1):
        # 指定 i元切词
        wordslist, lenStr = cutLine(text, i)
        # i元统计与合并（过滤频率大于等于2）
        filtered_dict = mergefix(wordslist, 2)
        # 单字概率
        if i == 1:
            one_gram_probability_dict = calculate_one_gram_probability(filtered_dict, lenStr)
            continue
        # 概率
        n_gram_probability_dict = calculate_one_gram_probability(filtered_dict, lenStr)  # 概率字典
        # 计算凝固度
        coagulation_dict = {}  # 凝固度字典
        for key in filtered_dict:
            coagulation_dict[key] = calculate_coagulation(key, n_gram_probability_dict, one_gram_probability_dict)
        # 计算自由度
        freedom_dict = freedom_dict_gen(filtered_dict)
        # 词新词 词 频率 凝固度 自由度
        sorted_words_info = words_info_merge(filtered_dict, n_gram_probability_dict, coagulation_dict, freedom_dict)
        # 不同元的词的凝固度差量级(粗略过滤)
        filtered_sorted_words = final_filter(sorted_words_info, freqthresheld, coagulationthresheldt, freedomthresheld, stopword_dict)

        all_word_list += filtered_sorted_words
    return all_word_list


def analyse_processing(inputpath, outputpath, freqthresheld, coagulationthresheldt, freedomthresheld, stopword_dict):

    print ""
    textRDD = sc.textFile(inputpath)
    # 网址 <----> 文本
    processed_RDD = textRDD.map(lambda s : s.split("\t")).map(lambda s : "\t".join([s[0], ";".join(calculate_processing(s[1], freqthresheld, coagulationthresheldt, freedomthresheld, stopword_dict))]))
    # 输出
    processed_RDD.repartition(4).saveAsTextFile(outputpath)


if __name__ == "__main__":

    sc = SparkContext(appName="analyse_processing")
    #calculate_processing(text, 3, 2, 0.2)
    stopword_dict = gen_stop_dict(sys.argv[3])
    broadcast_stopword_dict = sc.broadcast(stopword_dict)
    analyse_processing(sys.argv[1], sys.argv[2], 2, 2, 1.001, broadcast_stopword_dict.value)
