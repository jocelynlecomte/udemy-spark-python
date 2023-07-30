from operator import add

from pyspark.context import SparkContext

context = SparkContext(appName="Hello Spark", master="local[*]")

input_data = [35.5, 12.49943, 90.32, 20.32]
input_rdd = context.parallelize(input_data)

total = input_rdd.reduce(add)

print(f'total: {total}')

context.stop()
