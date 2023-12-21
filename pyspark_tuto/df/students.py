from pyspark.sql import SparkSession

session = (SparkSession.builder
           .appName("StudentsSQL")
           .master("local[*]")
           .getOrCreate())

studentsDF = session.read.option("header", True).csv("../resources/exams/students.csv")
# studentsDF.show()

studentsDF.createOrReplaceTempView("graded_students")

# gradedDF = studentsDF.where("subject == 'Modern Art' AND score > 50")  # expressions
gradedDF = studentsDF.where(studentsDF.subject == 'Modern Art').where(studentsDF.score > 50)  # columns
gradedDF.show()
