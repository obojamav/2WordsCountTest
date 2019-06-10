#!/usr/bin/env python

from pyspark import SparkContext
import collections

def mapper(all_data):
    map_list = []

    for line in all_data:

        w1 = line.split()
        w2 = collections.deque(words)
        w2.rotate(1)

        w1 = list(map(lambda a, b: a+ " " +b,list(w2), w1))
        del w1[0]

        map_list.append(w1)

    return map_list

def reducer(map_list):
    reducer_list = []
    for comp in map_list:
        for inx in comp:
            reducer_list.append(inx)

    return reducer_list

def main():
    sc = SparkContext(appName="2WordCount")
    input_file = sc.textFile('/user/cloudera/wz/test_wordcount.txt')
    token = input_file.flatMap(mapper)
    words = token.map(lambda word : ((word, 1)))
    wz = words.reduceByKey(lambda a, b: a + b)
    wz.saveAsTextFile('/user/cloudera/wz/output')
    sc.stop()

if __name__=='__main__':
    main()

