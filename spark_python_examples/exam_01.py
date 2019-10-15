# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/31 16:55
# @Author  : Despicable Me
# @Email   : 
# @File    : exam_01.py
# @Software: PyCharm
# @Explain :

from pyspark import SparkContext, SparkConf

# sc = SparkContext()

"""
Word Count:
In this example, we use a few transformations to build a 
dataset of (String, Int) pairs called counts and then save 
it to a file.
"""

# text_file = sc.textFile('D:\Projects_Python\spark\dataset')
# counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# # counts.saveAsTextFile("D:\Projects_Python\spark\dataset")
# print(counts.count())

"""
Pi Estimation:
Spark can also be used for compute-intensive tasks. This code 
estimates π by "throwing darts" at a circle. We pick random 
points in the unit square ((0, 0) to (1,1)) and see 
how many fall in the unit circle. The fraction should be π / 4, 
so we use this to get our estimate.
"""

# import random
#
# NUM_SAMPLES = 2000
#
# def inside(p):
#     x, y = random.random(), random.random()
#     return x*x + y*y < 1
#
# count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
#
# print('Pi is roughly %f' % (4.0 * count / NUM_SAMPLES))

"""
DataFrame API Examples
"""

"""
TEXT Search
"""

# import sqlite3
# from pyspark.sql import SparkSession
#
# sc = SparkContext()
#
# textFile = sc.textFile("hdfs://...")
#
# spark = SparkSession(sc)
#
# # Creates a DataFrame having a single column named 'line'
# df = textFile.map(lambda r: sqlite3.Row(r)).toDF(['line'])
# errors = df.filter(col('line').like("%ERROR%"))
#
# # Counts all the errors
# errors.count()
#
# # Counts errors mentioning MySQL
# errors.filter(col('line').like("%MYSQL")).count()
#
# # Fetches the MYSQL errors as an array of strings
# errors.filter(col('line').like("%MYSQL")).collect()

"""
Simple Data Operations
# Create a DataFrame based on a table named 'people'
# stored in a MYSQL database
"""
# from pyspark.sql import sqlContext
#
# url = 'jbdc:mysql://yourIP:yourPort/test?user=yourUsername;password=yourPassword'
# df = sqlContext \
#     .read \
#     .format('jbdc') \
#     .option('url', url) \
#     .option('dbtable', 'people') \
#     .load()
#
# # Lookd the schema of this DataFrame
# df.printSchema()
#
# #Counts people by age
# countsByAge = df.groupBy('age').count()
# countsByAge.show()
#
# # saves countsByAge to S3 in thr JSON format
# countsByAge.write.format('json').save("s3a://...")