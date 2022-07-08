from pyspark.sql import SparkSession
import pandas as pd


def create_Spark_DF():
    spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()
    spark_df = spark.read.csv("/Users/satyaprakash/PycharmProjects/Python_Spark/Data/*.csv", sep=',', header=True)
    return spark_df, spark

#
# spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()
#
# spark_df = spark.read.csv("/Users/satyaprakash/PycharmProjects/Python_Spark/Data/*.csv", sep=',', header=True)
# spark_df.groupBy("Stock_Name").count().show()
#
# spark_df.createOrReplaceTempView("table")
#
# print("Query - 1")
# sqlDF1 = spark.sql(
#     "SELECT t1.Date, t1.Stock_Name, ((t1.High-t1.Open)/t1.Open)*100 as Max_Pos_Pctg from table t1 where (("
#     "t1.High-t1.Open)/t1.Open)*100 = (Select Max(((t2.High-t2.Open)/t2.Open)*100) from table t2 WHERE t1.Date = "
#     "t2.Date)").show()
# sqlDF1 = spark.sql(
#     "SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 from table t1 where ((t1.Open-t1.Low)/t1.Open)*100 "
#     "= (Select Max(((t2.Open-t2.Low)/t2.Open)*100) from table t2 WHERE t1.Date = t2.Date)").show()
# sqlDF1 = spark.sql(
#     "SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 +((t1.High-t1.Open)/t1.Open)*100  from table t1 "
#     "where ((t1.Open-t1.Low)/t1.Open)*100 + ((t1.High-t1.Open)/t1.Open)*100 = (Select Max((("
#     "t2.Open-t2.Low)/t2.Open)*100 + ((t2.High-t2.Open)/t2.Open)*100)  from table t2 WHERE t1.Date = t2.Date)").show()
#
# print("Question - 2")
# sqlDF2 = spark.sql(
#     "SELECT t1.Date, t1.Stock_Name, t1.Volume FROM table t1 WHERE t1.Volume = (SELECT MAX(t2.Volume) FROM table t2 "
#     "WHERE t1.Date = t2.Date)")
# sqlDF2.show()
#
# # sqlDF2t = spark.sql("SELECT Date, Max(Volume) from table group by Date")
# # sqlDF2t.show()
#
# print("Query - 4")
# spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from table where "
#                    "Date='2017-07-10T00:00:00'")
#
# spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from table group by Stock_Name")
# spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join open_table t2 on t1.Stock_Name=t2.Stock_Name")
# print("join")
# spark.sql("select * from joined_table").show()
#
# print("join")
# spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)").show()
#
# print("Query - 5")
# sqlDF5 = spark.sql("SELECT Stock_Name, STD(Close) from table group by Stock_Name")
# sqlDF5.show()
#
# print("Query - 6.1")
# sqlDF61 = spark.sql("SELECT Stock_Name, AVG(Open) from table group by Stock_Name")
# sqlDF61.show()
#
# # print("Query - 6.2")
# # query62 = """SELECT *
# # FROM table t NATURAL JOIN (
# #   SELECT key, avg(val) as median
# #   FROM (
# #     SELECT key, val, rN, (CASE WHEN cN % 2 = 0 then (cN DIV 2) ELSE (cN DIV 2) + 1 end) as m1, (cN DIV 2) + 1 as m2
# #     FROM (
# #       SELECT key, val, row_number() OVER (PARTITION BY key ORDER BY val ) as rN, count(val) OVER (PARTITION BY key ) as cN
# #       FROM kvTable
# #          ) s
# #     ) r
# #   WHERE rN BETWEEN m1 and m2
# #   GROUP BY key
# # ) t"""
# #
# # sqlDF61 = spark.sql(query62)
# # sqlDF61.show()
#
# print("question - 7")
# sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) from table group by Stock_Name")
# sqlDF7.show()
#
# print("Question - 8")
# sqlDF8 = spark.sql("select Stock_Name, avg(Volume) as Average from table group by Stock_Name order by Average DESC "
#                    "limit 1")
# sqlDF8.show()
#
# print("Question - 9")
# sqlDF9 = spark.sql("select Stock_Name, MIN(Low), MAX(High) from table group by Stock_Name")
# sqlDF9.show()
