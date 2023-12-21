from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.types import IntegerType

session = (SparkSession.builder
           .appName("LogStatsSQL")
           .master("local[*]")
           .getOrCreate())

rawLogsDF = session.read.option("header", True).csv("../resources/biglog.csv")
# rawLogsDF.printSchema()

reworkedLogsDF = rawLogsDF.select(rawLogsDF.level,
                                  date_format(rawLogsDF.datetime, 'MMMM').alias("month"),
                                  date_format(rawLogsDF.datetime, 'M').cast(IntegerType()).alias('month_num'))

levelPivotByMonthDF = (
    reworkedLogsDF.groupBy(reworkedLogsDF.level)
    .pivot("month_num")
    .count().na.fill(0)
)

levelPivotByMonthDF.show()
