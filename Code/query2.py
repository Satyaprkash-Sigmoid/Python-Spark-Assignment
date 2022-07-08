from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Question - 2 -> Which stock was most traded stock on each day.")

sqlDF2 = spark.sql(
    "SELECT t1.Date, t1.Stock_Name, t1.Volume FROM table t1 WHERE t1.Volume = (SELECT MAX(t2.Volume) FROM table t2 "
    "WHERE t1.Date = t2.Date)")

sqlDF2.show()
