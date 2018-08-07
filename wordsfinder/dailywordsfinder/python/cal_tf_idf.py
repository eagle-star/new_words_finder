# _*_ coding:utf-8 _*_
"""
    计算tf idf
    @author kaolafm
    @create 2018-07-10 10:15
"""
import sys
import math
# sum_term_dict  file_num(通过dict size)  在多少个file中出现
# 词 总词频  N（多少文件中出现）
# 词 词总数 N（多少文件中出现）  idf  反 term/idf

def calcalate_sum(freq_dict, word, freq):

    if freq_dict.has_key(word):
        freq_dict[word] = freq_dict[word] + freq
    else:
        freq_dict[word] = freq

    return freq_dict

def statistic_files(freq_dict, word, n):
    if freq_dict.has_key(word):
        freq_dict[word] = freq_dict[word] + "," + n
    else:
        freq_dict[word] = n

    return freq_dict

def freq_statistic(inputpath):

    #"http://auto.qq.com/a/20180708/007481.htm"      汽车_5_0.0306748466258_5.05001553771_1.37095059445_34.616609017;价格_5_0.0306748466258_5.31304994354_2.0_53.1304994354;指南者_4_0.027397260274_11.2857347301_2.0_90.2858778408
    f = open(inputpath, 'r')
    line = f.readline()

    n = 0
    file_dict = {}
    word_freq_sum = {}
    word_in_files_freq = {}
    word_in_files_index = {}
    while line:
        it = line.split("\t")
        if len(it) != 2:
            line = f.readline()
            continue

        # 产生文件列表字典
        file_dict[n] = it[0].strip()

        word_infos = it[1].strip().split(";")

        for word_info in word_infos:
            infos = word_info.split("_")
            if len(infos) < 4:
                continue
            word = infos[0].strip()
            word_freq_sum = calcalate_sum(word_freq_sum, word, int(infos[1].strip()))
            word_in_files_freq = calcalate_sum(word_in_files_freq, word, 1)
            word_in_files_index = statistic_files(word_in_files_index, word, str(n))

        line = f.readline()
        n += 1

    #文档数量
    files_num =  len(file_dict)
    f.close()
    return files_num, file_dict, word_freq_sum, word_in_files_freq, word_in_files_index

def rever_tf_idf(file_num, word_freq_sum, word_in_files_freq_sum):

    return float(word_freq_sum) / math.log(float(file_num) / float(word_in_files_freq_sum), 2)

def check_url(file_dict, s):

    indexs = s.split(",")
    max_len = min(10, len(indexs))
    n = 0
    urls = []
    for i in indexs:
	if n == max_len:
	    break
        index = int(i.strip())
        if file_dict.has_key(index):
            urls.append(file_dict[index])
	n += 1
    return ",".join(urls)

def statics_process(inputpath):

    word_features = {}
    # 总文章数  文档编号   总词频数       文章出现数            具体哪些文档
    files_num, file_dict, word_freq_sum, word_in_files_freq, word_in_files_index = freq_statistic(inputpath)
    for key in word_freq_sum:
        term_freq = float(word_freq_sum[key])
        freq_in_files = float(word_in_files_freq[key])
	reverse_tfidf = rever_tf_idf(files_num, term_freq, freq_in_files)
	if freq_in_files >= 3.0 and reverse_tfidf >= 2.0:
            infos = "\t".join([str(term_freq), str(freq_in_files), str(reverse_tfidf), check_url(file_dict, word_in_files_index[key])])
            word_features[key] = infos
    return word_features

def new_words_pri(inputpath):
    """
    从大数据中查找数据
    :param inputpath:
    :return:
    """
    f = open(inputpath, 'r')
    line = f.readline()
    new_word_dict_pri = {}
    while line:
        word_info = line.split("\t")
        new_word_dict_pri[word_info[0].strip()] = 1
        line = f.readline()
    f.close()
    return new_word_dict_pri

def check_dict(s, word_features, new_words_4_out):

    if word_features.has_key(s):
        new_words_4_out[s] = word_features[s]

    return new_words_4_out

def write_out(outputpath, words_dict):
    out = open(outputpath, 'a+')
    for key in words_dict:
        out.write(key + "\t" + words_dict[key] + "\n")
    out.close

def check_new_words(inputpath, word_features):
    new_words_4_out = {}
    # 第一阶段跑出来的新词
    words_from_big_data = new_words_pri(inputpath)
    for key in words_from_big_data:
        if word_features.has_key(key):
            new_words_4_out[key] = word_features[key]
        else:
            new_words_4_out[key] = "from big data statistics"
    return new_words_4_out

def find_new_word(new_words_path_1, dict_output, filtered_hdfs_datapath, datapath3, old_dict_path):

    word_features = statics_process(datapath3)
    # 处理第一阶段的新词
    new_words_from_big_data = check_new_words(new_words_path_1, word_features)
    write_out(dict_output, new_words_from_big_data)
    # 处理第二阶段的新词
    words_from_daily_pri = new_words_pri(filtered_hdfs_datapath)

    old_dict = new_words_pri(old_dict_path)

    new_words_4_out = {}
    for key in words_from_daily_pri:
	if not new_words_from_big_data.has_key(key) and not old_dict.has_key(key):
            new_words_4_out = check_dict(key, word_features, new_words_4_out)

    write_out(dict_output, new_words_4_out)
    #return new_words_4_out


if __name__ == "__main__":

    #statics_process(sys.argv[1])

    find_new_word(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

