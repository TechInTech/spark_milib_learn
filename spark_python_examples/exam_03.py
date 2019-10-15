# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/31 22:07
# @Author  : Despicable Me
# @Email   : 
# @File    : exam_03.py
# @Software: PyCharm
# @Explain :

# try:
#     sc.stop()
# except:
#     pass
#
# from pyspark import SparkContext
#
# # 用户上网记录统计(一行为一条记录).（用户：第3列）
# sc = SparkContext(appName='test1')
# rdd = sc.textFile('..\dataset\\user_small.txt')\
#         .map(lambda x: x.split())\
#         .map(lambda x: (x[3], 1))\
#         .reduceByKey(lambda x,y: x+y)\
#         .map(lambda x:str(x[0])+' '+str(x[0][1])).collect()
#         #.saveAsTextFile('text1_1') #限定空格输出到文件
# print(rdd)

# 用户流量统计

def map_func(x):
    s = x.split()
    # return (s[2], [int(s[24]), int(s[25])]) # 第24、25列含有字符‘-’,故不能转化为整数
    return ([s[24]])

try:
    sc.stop()
except:
    pass

from pyspark import SparkContext

sc = SparkContext(appName='test')
lines = sc.textFile('..\dataset\\user_small.txt').map(lambda x: map_func(x)).collect()
print(lines)
# redByK = lines.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).collect()
# print(redByK)
# sum_flow = redByK.map(lambda x: str(x[0])+' '+str(x[1][0])+' '+str(x[1][1])).collect()#.saveAsTextFile('text1_2')
# print(sum_flow)
# sc.stop()