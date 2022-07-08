from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("question - 7 -> Find the average volume over the period")
sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) Average_volume from table group by Stock_Name")
sqlDF7.show()
