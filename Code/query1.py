from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Query - 1 --> On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)")
spark.sql(
    "CREATE TEMP VIEW positive_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.High-t1.Open)/t1.Open)*100 as Max_Pos_Pctg "
    "from table t1 where (( t1.High-t1.Open)/t1.Open)*100 = (Select Max(((t2.High-t2.Open)/t2.Open)*100) from table "
    "t2 WHERE t1.Date = t2.Date)")
spark.sql(
    "CREATE TEMP VIEW negative_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 from table t1 "
    "where ((t1.Open-t1.Low)/t1.Open)*100 = (Select Max(((t2.Open-t2.Low)/t2.Open)*100) from table t2 WHERE t1.Date = "
    "t2.Date)")

sqlDF1 = spark.sql("Select t1.Date, t1.Stock_Name as Highest, t2.Stock_Name as Lowest from positive_pctg t1 join "
                   "negative_pctg t2 on t1.Date=t2.Date")
sqlDF1.show()


