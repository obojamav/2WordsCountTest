#!/usr/bin/env python

from pyspark import SparkContext
import collections

def main():
    sc = SparkContext(appName="2WordCount")
    input_file = sc.textFile('/user/cloudera/wz/test_wordcount.txt')
    token = input_file.flatMap(lambda line:line.split())
    words = token.map(lambda word : (word, 1))
    wz = words.reduceByKey(lambda a, b: a + b)
    wz.saveAsTextFile('/user/cloudera/wz/output')
    sc.stop()

if __name__=='__main__':
    main()

