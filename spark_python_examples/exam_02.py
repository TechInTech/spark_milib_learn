# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/31 20:31
# @Author  : Despicable Me
# @Email   : 
# @File    : exam_02.py
# @Software: PyCharm
# @Explain :

def map_func(x):
    s = x.split()
    return (s[0], [int(s[1]), int(s[2]), int(s[3])])

def has100(x):
    for y in x:
        if(y == 100):
            return True
        return False

def allis0(x):
    if (type(x) == list and sum(x) == 0):
        return True
    return False

def subMax(x, y):
    m = [x[1][i] if (x[1][i] > y[1][i]) else y[1][i] for i in range(3)]
    return ('Maximum subject score', m)

def sumSub(x, y):
    n = [x[1][i] + y[1][i] for i in range(3)]
    return ('Total subject score', n)

def sumPer(x):
    return (x[0], sum(x[1]))

try:
    sc.stop()
except:
    pass

from pyspark import SparkContext

sc = SparkContext(appName='Student')
lines = sc.textFile('..\dataset\student.txt').map(lambda x: map_func(x)).cache()

count = lines.count()
print(count)

# RDD"转换"运算(筛选关键词filter)
whohas100 = lines.filter(lambda x: has100(x[1])).collect()
whois0 = lines.filter(lambda x: allis0(x[1])).collect()

print('whohas100: ', whohas100)
print('whois0: ', whois0)

# 动作运算
maxScore = max(lines.collect(), key=lambda x: x[1]) # 总分最高者
minScore = min(lines.collect(), key=lambda x: x[1]) # 总分最低者
# avgScore = [x[1]/count for x in lines.collect()] # 单科成绩平均值

print('maxScore: ', maxScore)
print('minScore: ', minScore)
# print('avgScore: ', avgScore)

# RDD key-value “转换”运算
subM = lines.reduce(lambda x,y: subMax(x, y))
sumSubScore = lines.reduce(lambda x,y: sumSub(x, y))
redByK = lines.reduceByKey(lambda x,y: [x[i]+y[i] for i in range(3)]).collect()

print('subM: ', subM)
print('sumSubScore: ', sumSubScore)
print('redByK: ', redByK)

# RDD “转换”运算
sumPerScore = lines.map(lambda x: sumPer(x)).collect()
sorted = lines.sortBy(lambda x: sum(x[1]))
sortedWithRank = sorted.zipWithIndex().collect()
first3 = sorted.takeOrdered(3, key=lambda x: -sum(x[1]))

print('sumPerScore: ', sumPerScore)
print('sorted: ', sorted.collect())
print('sortedWithRank: ', sortedWithRank)
print('first3: ', first3)

# 限定以空格的形式输出到文件中
first3RDD = sc.parallelize(first3).map(lambda x: str(x[0])+''+str(x[1][0])\
                     +''+str(x[1][1])+''+str(x[1][2])).saveAsTextFile('result')
print(first3RDD)

sc.stop()