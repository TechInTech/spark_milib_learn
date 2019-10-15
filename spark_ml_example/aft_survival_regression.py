# !/usr/bin/env python
# -*- coding:utf-8 -*-

# @Time    : 2019/7/31 23:08
# @Author  : Despicable Me
# @Email   : 
# @File    : aft_survival_regression.py
# @Software: PyCharm
# @Explain :
from pyspark.ml.regression import AFTSurvivalRegression
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('AFTSurvivalRegressionExample') \
        .getOrCreate()

    training = spark.createDataFrame([
        (1.218, 1.0, Vectors.dense(1.560, -0.605)),
        (2.949, 0.0, Vectors.dense(0.346, 2.158)),
        (3.627, 0.0, Vectors.dense(1.380, 0.231)),
        (0.273, 1.0, Vectors.dense(0.520, 1.151)),
        (4.199, 0.0, Vectors.dense(0.795, -0.226))], \
        ['label', 'censor', 'features'])

    quantileProbalities = [0.3, 0.6]
    aft = AFTSurvivalRegression(quantileProbabilities=quantileProbalities,
                                quantilesCol='quantiles')

    model = aft.fit(training)

    print('Coefficients: ' + str(model.coefficients))
    print('Intercept: ' + str(model.intercept))
    print('Scale: ' + str(model.scale))
    model.transform(training).show(truncate=False)

    spark.stop()