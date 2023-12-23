from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


def has_passed(grade: str):
    if grade.startswith("A") or grade.startswith("B") or grade.startswith("C"):
        return True
    else:
        return False


session = (SparkSession.builder
           .appName("StudentsSQL")
           .master("local[*]")
           .getOrCreate())

has_passed_udf = udf(lambda grade: has_passed(grade), BooleanType())

studentsDF = session.read.option("header", True).csv("../resources/exams/students.csv")
# studentsDF.show()

gradedDF = studentsDF.withColumn("passed", has_passed_udf(studentsDF.grade))
gradedDF.show()
