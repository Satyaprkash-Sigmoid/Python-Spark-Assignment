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

    spark.sql(
        "CREATE TEMP VIEW positive_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.High-t1.Open)/t1.Open)*100 as "
        "Max_Pos_Pctg from table t1 where (( t1.High-t1.Open)/t1.Open)*100 = (Select Max((("
        "t2.High-t2.Open)/t2.Open)*100) from table t2 WHERE t1.Date = t2.Date)")
    spark.sql(
        "CREATE TEMP VIEW negative_pctg AS SELECT t1.Date, t1.Stock_Name, ((t1.Open-t1.Low)/t1.Open)*100 from table t1 "
        "where ((t1.Open-t1.Low)/t1.Open)*100 = (Select Max(((t2.Open-t2.Low)/t2.Open)*100) from table t2 WHERE "
        "t1.Date = t2.Date)")

    sqlDF1 = spark.sql(
        "Select t1.Date, t1.Stock_Name as Highest, t2.Stock_Name as Lowest from positive_pctg t1 join negative_pctg "
        "t2 on t1.Date=t2.Date").toPandas()

    my_output = sqlDF1.to_dict('records')
    output["stock has moved maximum %age wise in both directions - "] = my_output

    return output


if __name__ == '__main__':
    app.run(debug=True, port=2001)
