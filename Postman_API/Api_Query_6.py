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

    sqlDF6 = spark.sql(
        "Select Stock_Name, avg(open) as Mean, percentile_approx(open,0.5) as Median from table group by "
        "Stock_Name").toPandas()

    my_output = sqlDF6.to_dict('records')
    output["mean  and median prices - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2006)
