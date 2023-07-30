from operator import add

from pyspark import SparkContext

from pyspark_tuto.common.helper_functions import swap_tuple, count_init

context = SparkContext(appName="Hello Spark", master="local[*]")

with open('../resources/subtitles/boringwords.txt') as boring_words_file:
    boring_words = [line.strip() for line in boring_words_file.readlines()]

cleaned_subtitles = context.textFile('../resources/subtitles/input.txt') \
    .filter(lambda line: len(line.strip()) > 0) \
    .filter(lambda line: '-->' not in line) \
    .filter(lambda line: not line.isdigit()) \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: word.replace('.', '')) \
    .map(lambda word: word.replace(',', '')) \
    .map(lambda word: word.replace('[', '')) \
    .map(lambda word: word.replace(']', '')) \
    .map(lambda word: word.replace('\'s', '')) \
    .map(lambda word: word.replace('\'', '')) \
    .map(lambda word: word.lower()) \
    .filter(lambda word: word not in boring_words)

keyword_rankings = cleaned_subtitles \
    .map(count_init) \
    .reduceByKey(add) \
    .map(swap_tuple) \
    .sortByKey(ascending=False) \
    .take(10)

for keyword_ranking in keyword_rankings:
    print(keyword_ranking)

context.stop()
