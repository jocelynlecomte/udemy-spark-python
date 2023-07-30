from math import sqrt

from pyspark.context import SparkContext

context = SparkContext(appName="Hello Spark", master="local[*]")

input_data = [35, 12, 90, 20]
input_rdd = context.parallelize(input_data)

sqrt_rdd = input_rdd.map(sqrt)

sqrt_list = sqrt_rdd.collect()
for sqrt_val in sqrt_list:
    print(f'square root: {sqrt_val}')

context.stop()
