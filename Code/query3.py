from create_DF import create_Spark_DF


spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Question - 3 -> ")

sqlDF3 = spark.sql("")

sqlDF3.show()