from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Question - 3 -> previous day close -  current day open price")
spark.sql("CREATE TEMP VIEW lagged_table AS SELECT *, Lag(Close, 1, 0) OVER(PARTITION BY Stock_Name ORDER BY "
          "Date ASC) AS prev_Close FROM table")
spark.sql("Select * from lagged_table").show()

sqlDF3 = spark.sql("select t1.Stock_Name, Abs(t1.prev_Close-t1.Open) from lagged_table t1 where Abs("
                   "t1.prev_Close-t1.Open) = (Select Max(Abs(t2.prev_Close-t2.Open)) from lagged_table t2)")

sqlDF3.show()
