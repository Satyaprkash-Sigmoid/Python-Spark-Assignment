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

    sqlDF2 = spark.sql(
        "SELECT t1.Date, t1.Stock_Name, t1.Volume FROM table t1 WHERE t1.Volume = (SELECT MAX(t2.Volume) FROM table t2 "
        "WHERE t1.Date = t2.Date)").toPandas()

    my_output = sqlDF2.to_dict('records')
    output["stock was most traded stock on each day - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2002)
