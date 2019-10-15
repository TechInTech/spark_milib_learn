# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/27 17:36
# @Author  : Despicable Me
# @Email   : 
# @File    : 02.py
# @Software: PyCharm
# @Explain :

from operator import gt
from pyspark.sql import SparkSession


class Girl:
    def __init__(self, faceValue, age):
        self.faceValue = faceValue
        self.age = age

    def __gt__(self, other):
        if other.faceValue == self.faceValue:
            return gt(self.age, other.age)
        else:
            return gt(self.faceValue, other.faceValue)


if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .master("local")\
            .getOrCreate()
    sc = spark.sparkContext
    rdd1 = sc.parallelize([("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)])
    rdd2 = rdd1.sortBy(lambda das: Girl(das[1], das[2]),False)
    print(rdd2.collect())
    sc.stop()
