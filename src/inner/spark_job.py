# Этот модуль применяет фильтры для вывода результата пользователю
# с помощью Spark (по дате и ключевым словам в тексте)

import re
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
from datetime import datetime as dt

# Функции-обертки для СУБД
from inner.csv2db import start, fin, show

# Основная "точка входа"
def run(date1, date2, terms, toCsv=True):
    # Создаем контекст Spark
    spark = SparkSession.builder.appName("ArticleTransformation").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    sqlContext.sql("set spark.sql.shuffle.partitions=2")

    # Загружаем данные из СУБД в датафрейм Spark
    start()
    df = spark.createDataFrame(show('articles'))
    fin()

    # Фильтрация по ключевым словам, если они есть
    words_clean = None
    if terms:
        words_clean = '|'.join(re.compile(r'\w+').findall(terms.lower()))

    # Выражение для поиска по ключевым словам
    search = None
    if words_clean:
        search = f" lower(abstract) rlike '{words_clean}' "

    # Подготавливаем DataFrame
    df_result = df.select(F.col("*"), F.to_timestamp(F.col("date"), "dd/MM/yyyy").alias("to_timestamp"))

    # Фильтрация по датам
    if date1 and date2:
        df_result = df_result.filter(F.col("to_timestamp").between(date1, date2))

    # Фильтрация по ключевым словам
    if search:
        df_result = df_result.filter(search)

    # Сортировка результатов
    df_result = df_result.sort(F.desc("to_timestamp")).toPandas()

    # Закрываем Spark
    spark.stop()

    # Сохраняем результат в CSV, если требуется
    if toCsv:
        df_result.to_csv("spark_job.csv", index=False)

    return df_result
