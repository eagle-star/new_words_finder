# _*_ coding:utf-8 _*_
"""
    查找新词
    @author kaolafm
    @create 2018-06-28 18:59
"""

import sys

def add_old_dict(inputpath):
    """
    昨天的字典放入dict中
    :param inputpath:
    :return:
    """
    olddict = {}

    f = open(inputpath, 'r')
    line = f.readline()
    n = 0
    while line:
        n += 1
        it = line.split("\t")
        if len(it) != 5:
            line = f.readline()
            continue

        olddict[it[0].strip()] = line
        line = f.readline()

        if n % 2000 == 0:
            print "add yesterday dict ", n, " in dict"
    f.close()
    return olddict

def check_new_dict(newdictpath, olddictpath, newwordspath):

    olddict = add_old_dict(olddictpath)
    f = open(newdictpath, 'r')
    out = open(newwordspath, 'a+')
    line = f.readline()
    while line:
        it = line.split("\t")
        if len(it) != 5:
            line = f.readline()
            continue

        word = it[0].strip()
        if not olddict.has_key(word):
            out.write(line)
        line = f.readline()
    f.close()
    out.close()


if __name__ == '__main__':

    check_new_dict(sys.argv[1], sys.argv[2], sys.argv[3])
