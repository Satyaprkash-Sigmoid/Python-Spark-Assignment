from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Question - 8 -> Find which stock has higher average volume")
sqlDF8 = spark.sql("select Stock_Name, avg(Volume) as Max_Average_Volume from table group by Stock_Name order by "
                   "Max_Average_Volume DESC limit 1")
sqlDF8.show()
