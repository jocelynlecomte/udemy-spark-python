from pyspark.sql import SparkSession
from pyspark.sql import functions as F

session = (SparkSession.builder
           .appName("StudentsSQL")
           .master("local[*]")
           .getOrCreate())

studentsDF = session.read.option("header", True).csv("../resources/exams/students.csv")
# studentsDF.show()

maxNoteBySubjectDF = studentsDF.groupBy(studentsDF.subject).agg(F.max(studentsDF.score))
# maxNoteBySubjectDF = studentsDF.groupBy(studentsDF.subject).agg({"score": "max"})

maxNoteBySubjectDF.show()
