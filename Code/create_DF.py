from pyspark.sql import SparkSession


def create_Spark_DF():
    spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()
    spark_df = spark.read.csv("/Users/satyaprakash/PycharmProjects/Python_Spark/Data/*.csv", sep=',', header=True)
    return spark_df, spark
