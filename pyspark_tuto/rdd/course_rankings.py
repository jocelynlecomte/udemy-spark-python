import array
import string
from operator import add

from pyspark import SparkContext

from pyspark_tuto.common.helper_functions import swap_tuple, count_init


def split_csv_line(line: string) -> array:
    return line.split(',')


def compute_score(viewed_chapters: int, chapter_count: int) -> float:
    ratio = (viewed_chapters * 1.0) / chapter_count
    if ratio > 0.9:
        return 10
    elif ratio > 0.5:
        return 4
    elif ratio > 0.25:
        return 2
    else:
        return 0


context = SparkContext(appName="Hello Spark", master="local[*]")

chapters = context.textFile('../resources/viewing_figures/chapters.csv') \
    .map(split_csv_line)  # (chapterId, courseId)

# print(chapters.collect())

titles = context.textFile('../resources/viewing_figures/titles.csv') \
    .map(split_csv_line)  # (courseId, title)

views = context.textFile('../resources/viewing_figures/views-*.csv') \
    .map(split_csv_line) \
    .map(lambda a: (a[0], a[1])) \
    .map(swap_tuple) \
    .distinct()  # (chapterId, userId)

chapter_count_by_course = chapters \
    .map(lambda t: t[1]) \
    .map(count_init) \
    .reduceByKey(add) \
    .map(swap_tuple) \
    .sortByKey(ascending=False) \
    .map(swap_tuple)  # (courseId, chapterCount)

# print(chapter_count_by_course.collect())

# join chapters and views
chapters_views_join = chapters \
    .join(views) \
    .map(swap_tuple)  # ((courseId, userId), chapterId)

# count how many lines with same (userId, courseId)
viewed_chapters_by_course_by_user = chapters_views_join \
    .map(lambda t: t[0]) \
    .map(count_init) \
    .reduceByKey(add) \
    .map(lambda t: (t[0][0], t[1]))  # (courseId, viewedChapters))

# print(f'viewed_chapters_by_course_by_user {viewed_chapters_by_course_by_user.collect()}')

# join with chapterCountByCourse
scores_by_course = viewed_chapters_by_course_by_user \
    .join(chapter_count_by_course) \
    .map(lambda t: (t[0], compute_score(t[1][0], t[1][1]))) \
    .reduceByKey(add)  # (courseId, score)

# print(f'scores_by_course {scores_by_course.collect()}')

# join with titles
scores_by_course_titled = scores_by_course \
    .join(titles) \
    .map(lambda t: t[1]) \
    .sortByKey(ascending=False) \
    .map(swap_tuple)

print(f'scores_by_course_titled {scores_by_course_titled.collect()}')

context.stop()
