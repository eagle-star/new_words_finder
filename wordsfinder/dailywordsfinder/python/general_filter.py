# _*_ coding:utf-8 _*_
"""
    
    @author kaolafm
    @create 2018-07-12 12:59
"""

import re
import sys


def char_fliter(s):

    if s.islower():
        return False
    else:
        return True

def neg_words(inputpath):

    f = open(inputpath, 'r')
    line = f.readline()
    neg_words_dict = {}

    while line:
	if len(line.strip().split("\t")) >= 4:
	    neg_word = line.split("\t")[0].strip()
	    neg_words_dict[neg_word] = 1
	line = f.readline()
    f.close
    return neg_words_dict

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
    
    f.close
    return pos_stop_words_dict

def stop_words():

    stop_words_dict = {}
    stop_words_list = ['该', '为', '对', '有', '我', '个', '称', '也', '了', '是', '和', '在', '的', '你', '这']
    for word in stop_words_list:
	stop_words_dict[word] = 1
    return stop_words_dict

def ischeckedword(word, pos_stop_words_dict, stop_words_dict):
	
    n = 1
    for key in stop_words_dict:
	if key not in word:
	    n *= 1
	else:
	    n *= 0
    if n == 1:
	return True

    if pos_stop_words_dict.has_key(word):
        return True
    else:
        return False

def num_filter(word):

    if bool(re.search(r'\d', word)):
        return False
    else:
        return True

def read_file(inputpath, pos_stop_words_dict, stop_words_dict, neg_words_dict):

    f = open(inputpath, 'r')
    line = f.readline()

    while line:
        it = line.split("\t")
        word = it[0].strip()
        if len(it) < 4:
            line = f.readline()
            continue
        if char_fliter(word) and num_filter(word) and ischeckedword(word, pos_stop_words_dict, stop_words_dict) and not neg_words_dict.has_key(word):
            print line
        line = f.readline()

    f.close()

neg_words_dict = neg_words(sys.argv[3])
stop_words_dict = stop_words()
pos_stop_words_dict = gen_stop_dict(sys.argv[2])

read_file(sys.argv[1], pos_stop_words_dict, stop_words_dict, neg_words_dict)


