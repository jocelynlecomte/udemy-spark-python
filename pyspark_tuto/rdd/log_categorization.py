from operator import add

from pyspark.context import SparkContext

from pyspark_tuto.common.helper_functions import count_init

context = SparkContext(appName="Hello Spark", master="local[*]")

input_data = ["WARN: Tuesday 4 September 0405",
              "ERROR: Tuesday 4 September 0408",
              "FATAL: Wednesday 5 September 1632",
              "ERROR: Friday 7 September 1854",
              "WARN: Saturday 8 September 1942"]
original_log_messages = context.parallelize(input_data)

log_count_by_severity = original_log_messages \
    .map(lambda line: line.split(':')) \
    .map(lambda chunk: chunk[0]) \
    .map(count_init) \
    .reduceByKey(add) \
    .collect()

for count in log_count_by_severity:
    print(count)

context.stop()
