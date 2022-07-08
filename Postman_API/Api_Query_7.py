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

    sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) Average_volume from table group by Stock_Name").toPandas()

    my_output = sqlDF7.to_dict('records')
    output["average volume "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2007)
