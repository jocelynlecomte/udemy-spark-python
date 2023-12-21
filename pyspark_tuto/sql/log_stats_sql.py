from pyspark.sql import SparkSession

session = (SparkSession.builder
           .appName("LogStatsSQL")
           .master("local[*]")
           .getOrCreate())

rawLogsDF = session.read.option("header", True).csv("../resources/biglog.csv")
# rawLogsDF.printSchema()

rawLogsDF.createOrReplaceTempView("raw_logs")

query = ("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
         "from raw_logs group by level, month "
         "order by cast(first(date_format(datetime, 'M')) as int), level")
results = session.sql(query)

results.show(100)
