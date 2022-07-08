from create_DF import create_Spark_DF

spark_df, spark = create_Spark_DF()
spark_df.createOrReplaceTempView("table")

print("Query - 4 - Which stock has moved maximum from 1st Day data to the latest day Daya")

print("Tables having Only Open Price from each stock at first Day")
spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from table where "
          "Date='2017-07-10T00:00:00'")
spark.sql("select * from open_table").show()

print("Tables having Maximum high from each stock")
spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from table group by Stock_Name")
spark.sql("select * from high_table").show()

spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join "
          "open_table t2 on t1.Stock_Name=t2.Stock_Name")

print("join of Above two Tables :- ")
spark.sql("select * from joined_table").show()

print("Final Desired Output : ")
sqlDF4 = spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where "
                   "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)")
sqlDF4.show()
