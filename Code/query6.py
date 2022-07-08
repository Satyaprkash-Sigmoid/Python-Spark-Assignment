from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Query - 6.1 -> Mind the mean  and median prices for each stock")

sqlDF6 = spark.sql("Select Stock_Name, avg(open) as Mean, percentile_approx(open,0.5) as Median from table group by "
                   "Stock_Name")

sqlDF6.show()
