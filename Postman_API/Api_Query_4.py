import datetime
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import pandas as pd

app = Flask(__name__)


@app.route("/result", methods=["GET"])
def result():
    data1 = fun()
    return jsonify(data1)


def fun():
    output = {}

    spark = SparkSession.builder.appName('Read Many Stocks Details').getOrCreate()

    spark_df = spark.read.csv("/Users/satyaprakash/PycharmProjects/Python_Spark/Data/*.csv", sep=',', header=True)

    spark_df.createOrReplaceTempView("table")

    spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from table where "
              "Date='2017-07-10T00:00:00'")

    print("Tables having Maximum high from each stock")
    spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from table group by Stock_Name")

    spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join "
              "open_table t2 on t1.Stock_Name=t2.Stock_Name")

    spark.sql("select * from joined_table").show()

    sqlDF4 = spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where "
                       "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)").toPandas()

    my_output = sqlDF4.to_dict('records')
    output["stock has moved maximum from 1st Day data to the latest day Data - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2004)
