from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

session = (SparkSession.builder
           .appName("LogStatsSQL")
           .master("local[*]")
           .getOrCreate())

rawLogsDF = session.read.option("header", True).csv("../resources/biglog.csv")
# rawLogsDF.printSchema()

withMonthNumDF = rawLogsDF.withColumn("month_num", date_format(rawLogsDF.datetime, 'M'))

reworkedLogsDF = withMonthNumDF.select(withMonthNumDF.level,
                                       date_format(withMonthNumDF.datetime, 'MMMM').alias("month"),
                                       withMonthNumDF.month_num)

groupedByLevelByMonthDF = (
    reworkedLogsDF.groupBy(reworkedLogsDF.level, reworkedLogsDF.month, reworkedLogsDF.month_num)
    .count()
    .orderBy(reworkedLogsDF.month_num, reworkedLogsDF.level))

groupedByLevelByMonthDF.drop(groupedByLevelByMonthDF.month_num).show()
