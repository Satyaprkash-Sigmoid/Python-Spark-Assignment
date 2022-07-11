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

    spark.sql("CREATE TEMP VIEW lagged_table AS SELECT *, Lag(Close, 1, 0) OVER(PARTITION BY Stock_Name ORDER BY "
              "Date ASC) AS prev_Close FROM table")
    spark.sql("Select * from lagged_table").show()

    sqlDF3 = spark.sql("select t1.Stock_Name, Abs(t1.prev_Close-t1.Open) Max_Gap from lagged_table t1 where Abs("
                       "t1.prev_Close-t1.Open) = (Select Max(Abs(t2.prev_Close-t2.Open)) from lagged_table t2)").toPandas()

    my_output = sqlDF3.to_dict('records')
    output["stock was most traded stock on each day - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2003)
