from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, round

session = (SparkSession.builder
           .appName("StudentsSQL")
           .master("local[*]")
           .getOrCreate())

studentsDF = session.read.option("header", True).csv("../resources/exams/students.csv")
# studentsDF.show()

aggByYearBySubjectDF = (studentsDF
                        .groupBy(studentsDF.subject)
                        .pivot("year")
                        .agg(round(avg(studentsDF.score), 2).alias("average"),
                             round(stddev(studentsDF.score), 2).alias("deviation")
                             )
                        )

aggByYearBySubjectDF.show()
