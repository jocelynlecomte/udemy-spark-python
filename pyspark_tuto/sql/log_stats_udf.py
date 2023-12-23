from time import strptime

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def month_num(month_name: str):
    return strptime(month_name, '%B').tm_mon


session = (SparkSession.builder
           .appName("LogStatsSQL")
           .master("local[*]")
           .getOrCreate())

session.udf.register("month_num", month_num, IntegerType())

rawLogsDF = session.read.option("header", True).csv("../resources/biglog.csv")
# rawLogsDF.printSchema()

rawLogsDF.createOrReplaceTempView("raw_logs")

query = ("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
         "from raw_logs group by level, month "
         "order by month_num(month), level")
results = session.sql(query)

results.show(100)
