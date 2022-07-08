from create_DF import create_Spark_DF


spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")


print("Question - 9 -> Find the highest and lowest prices for a stock over the period of time")
sqlDF9 = spark.sql("select Stock_Name, MIN(Low) AS Highest_Price, MAX(High) AS Lowest_Price from table group by "
                   "Stock_Name")
sqlDF9.show()