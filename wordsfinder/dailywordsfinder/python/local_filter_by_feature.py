# _*_ coding:utf-8 _*_
"""
    
    @author kaolafm
    @create 2018-07-10 9:48
"""

# _*_ coding:utf-8 _*_
"""
    
    @author kaolafm
    @create 2018-07-06 18:07
"""
import math
import sys

def seperator(inputpath):

    f = open(inputpath, 'r')
    line = f.readline()
    wordslist_2 = []
    wordslist_3 = []
    wordslist_4 = []
    #wordslist_5 = []
    while line:
        it = line.split("\t")
        #做一层纯数字的过滤
        if len(it) != 2:
            line = f.readline()
            continue
        itt = it[1].split(";")
        for words in itt:
            it = words.split("_")
	    wordlen = str(it[0]).decode("utf-8")
	    if len(it) != 6:
                continue
            wordsinfo = it[0] + "_" + it[1] + "_" + it[2] + "_" + it[3] + "_" + it[4].strip() + "_" + str(int(it[1])*float(it[3])*float(it[4]))
            if len(wordlen) == 2:
                wordslist_2.append(wordsinfo + "\n")
            elif len(wordlen) == 3:
                wordslist_3.append(wordsinfo + "\n")
            elif len(wordlen) == 4:
                wordslist_4.append(wordsinfo + "\n")
            #elif len(wordlen) == 5:
                #wordslist_5.append(wordsinfo + "\n")
        line = f.readline()
    print len(wordslist_2),"-------",len(wordslist_3),"-------",len(wordslist_4)
    return [wordslist_2, wordslist_3, wordslist_4]

def sorted_words(wordslist, index):

    return sorted(wordslist, key=lambda x : x.split("_")[index], reverse=True)

def isWord(feature, freqshresheld, coagulationthresheld, freedomshresheld, factthresheld):

    word_features = feature.split("_")
    flag = False
    if len(word_features) == 6:
	try:
            if int(word_features[1]) >= int(freqshresheld) and float(word_features[3]) >= float(coagulationthresheld) and float(word_features[4]) >= float(freedomshresheld) and float(word_features[5]) > float(factthresheld):
                flag = True
	except Exception, e:
	    print " ".join(word_features)
	    print e
    return flag

def filter_words(wordslist, freqshresheld, coagulationthresheld, freddomshresheld, factshresheld):

    filtered_words = []    
    for word in wordslist:
	if isWord(word, freqshresheld, coagulationthresheld, freddomshresheld, factshresheld):
	    filtered_words.append(word)
    return filtered_words


def filter_process(wordslist, freqshresheld, coagulationthresheld, freddomshresheld, factshresheld):

    filtered_words = filter_words(wordslist, freqshresheld, coagulationthresheld, freddomshresheld, factshresheld)
    sorted_filtered_words = sorted_words(filtered_words, index=5)
    return sorted_filtered_words

def read_params_file(inputpath):

    f = open(inputpath, 'r')
    line = f.readline()
    params = []
    while line:
        n_params = line.split("|")
        params.append(n_params)
        line = f.readline()
    return params

def write_out(outputpath, wordslist):
   
    out = open(outputpath, 'a+')
    for word in wordslist:
	replaced_words = word.replace("_", "\t")
	out.write(replaced_words)

    out.close	

if __name__ == "__main__":

    wordslist_list = seperator(sys.argv[1])
    params = read_params_file(sys.argv[2])

    for i in range(2, 5, 1):
        wordslist = wordslist_list[i-2]

        freqshresheld = params[i-2][0].strip()
        coagulationthresheld = params[i-2][1].strip()
        freddomshresheld = params[i-2][2].strip()
        factshresheld = params[i-2][3].strip()
        sorted_filtered_words = filter_process(wordslist, freqshresheld, coagulationthresheld, freddomshresheld, factshresheld)
	
	outputpath = sys.argv[3] + "_" + str(i)
        write_out(outputpath, sorted_filtered_words)
        

