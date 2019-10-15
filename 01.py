# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/27 15:52
# @Author  : Despicable Me
# @Email   : 
# @File    : 01.py
# @Software: PyCharm
# @Explain :

from pyspark import SparkContext

sc = SparkContext('local')
doc = sc.parallelize([['a','b','c'],['b','d','d']])
words = doc.flatMap(lambda d:d).distinct().collect()
word_dict = {w:i for w,i in zip(words,range(len(words)))}
word_dict_b = sc.broadcast(word_dict)

def wordCountPerDoc(d):
    dict={}
    wd = word_dict_b.value
    for w in d:
        if wd[w] in dict.keys():
            dict[wd[w]] +=1
        else:
            dict[wd[w]] = 1
    return dict
print(doc.map(wordCountPerDoc).collect())
print("successful!")

# from pyspark import SparkContext
#
# sc = SparkContext(master='local[2]',appName='pyspark')
#
# rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
#
# print(rdd.collect())
#
# sc.stop()
#
# from pyspark import SparkContext
# logFile = "D:\Develop\Spark\README.md"
# sc = SparkContext("local","Simple App")
# logData = sc.textFile(logFile).cache()
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()
# print("Lines with a: %i,lines with b: %i"%(numAs,numBs))

# #-*- coding: utf-8 -*-
# __author__ = 'kaylee'
#
# import os
# import sys
#
# os.environ['SPARK_HOME'] = "D:\Develop\Spark"
# sys.path.append("D:\Develop\Spark\python")
#
# try:
#     from pyspark import SparkContext
#     from pyspark import SparkConf
#
#     print('Successfully imported Spark Modules')
# except ImportError as e:
#     print('Can not import Spark Modules', e)
# sys.exit(1)
