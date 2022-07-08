from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Query - 5 -> Find the standard deviations for each stock over the period")
sqlDF5 = spark.sql("SELECT Stock_Name, STD(Close) as Standard_Deviation from table group by Stock_Name")
sqlDF5.show()
