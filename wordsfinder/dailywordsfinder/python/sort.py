# _*_ coding:utf-8 _*_

import sys

def sort_file(inputpath, outputpath):
    
    f = open(inputpath, 'r')
    line = f.readline()
    array = []

    while line:
	it = line.split("\t")
	if len(it) != 5:
	    line = f.readline()
	    continue
	array.append(line)
	line = f.readline()

    array_by_index = sorted(array, key=lambda x : int(x.split("\t")[int(1)]), reverse=True)

    out = open(outputpath, 'a+')
    for l in array_by_index:
	
	out.write(l)

    f.close()
    out.close()

if __name__ == "__main__":
    
    sort_file(sys.argv[1], sys.argv[2])

