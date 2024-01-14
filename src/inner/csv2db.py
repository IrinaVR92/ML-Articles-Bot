# Этот модуль объединяет CSV-файлы и загружает из в базу данных
# Выбрана PostgreSQL, если потребуется, можно будет адаптировать
# для другой реляционной СУБД

import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
import datetime

### Блок функций-оберток для работы с PostgreSQL

FN = "db_settings.inf"

def load_sql_params():
	try:
		return {key: str(value[0]) for key, value in pd.read_csv(FN).to_dict().items()}
	except Exception as e: # не очень хорошо, но и нет большого смысла искать конкретные причины неудачи
		print("Sql parameters error:", e)
		return None # продолжаем - скорее всего, база просто еще не проинициализирована

SQL_PARAMS = load_sql_params()

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS articles (
	code VARCHAR(255) PRIMARY KEY,
	url TEXT,
	title TEXT,
	authors TEXT,
	abstract TEXT,
	tags TEXT,
	date VARCHAR(10),
	src VARCHAR(10)
);
""".strip("\n") #убрали последние \n с начала и с конца

# Подключение к базе
def start(params=SQL_PARAMS):
    global conn
    global cursor
    conn = psycopg2.connect(**params)
    conn.autocommit = False
    cursor = conn.cursor()

# Закрытие подключения к базе
def fin():
    cursor.close()
    conn.close()

# Безопасное выполнение команды/скрипта SQL
def apply(script, *args):
    global conn
    global cursor    
    try:
        cursor.execute(script, args)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

# Обертка на случай более изощренных схем
def table_fullname(table):
    return table

# Выполнение скрипта SQL из файла
def apply_file(fn):
    with open(fn, "r", encoding="utf-8") as f:
        script = f.read()
    apply(script)
    
# Загрузка таблицы или команды SELECT как команды;
# не самая безопасная версия (в плане SQL-вставок),
# но для текущих целей сойдет.
def show(table_name, as_command=False):
    global conn
    global cursor
    command = table_name if as_command else f"SELECT * FROM {table_fullname(table_name)};"    
    cursor.execute(command)
    df = pd.DataFrame([c for c in cursor.fetchall()], columns=[desc[0] for desc in cursor.description])
    return df

def drop_db(): # Для теста, удаление таблицы
	start()
	apply("DROP TABLE articles");
	fin()

# Инициализация БД
def init_db():
	COLS = "host, database, port, user, password".split(", ")
	VALS = "localhost, postgres, 5433, postgres, 1".split(", ")	
	pd.DataFrame([VALS], columns=COLS).to_csv(FN, index=False)
	start(params=load_sql_params())
	apply(CREATE_TABLE) # создаем нашу таблицу для хранения данных (articles)
	fin()
    
### Блок функций-оберток для работы с PostgreSQL завершен

# Приведение даты DD/MM/YYYY к datetime-совместимому формату (типа YYYY-MM-DD)
def date_conv(d):
    return datetime.date(*map(int, d.split("/")[::-1]))

# Убеждаемся, что переданная дата находится в заданном диапазоне
def date_valid(d, d1, d2):
    f = False
    try:
        f = d1 <= date_conv(d) <= d2
    except:
        print("Unexpected/Unexisting date:", d, ", skipping")
    finally:
        return f

# Слияние двух источников данных в один CSV-файл
def total(date1, date2):
	df_a = pd.read_csv('arxiv.csv')
	df_c = pd.read_csv('cwp.csv')

	df_a['src'] = 'arxiv'
	df_c['src'] = 'cwp'

	d1 = datetime.date(*map(int, date1.split('-')))
	d2 = datetime.date(*map(int, date2.split('-')))

	df_a1 = df_a[df_a['date'].apply(date_valid, args=(d1, d2))]
	df_c1 = df_c[df_c['date'].apply(date_valid, args=(d1, d2))]

	pd_united = pd.concat([df_a1, df_c1])
	pd_united['date_sort'] = pd_united['date'].apply(date_conv)
	pd_united['title_lower'] = pd_united['title'].apply(lambda s: s.lower())
	df_fin = pd_united.sort_values(by=['title_lower'], ascending=True).sort_values(by=['date_sort'], ascending=False).drop(columns=['title_lower', 'date_sort']).reset_index(drop=True)
    
	date_restore = lambda dtt: '/'.join([str(int(i)).zfill(2 if n < 2 else 4) for n, i in enumerate(dtt.split("/"))])
	df_fin['date'] = df_fin['date'].apply(date_restore)
    
	df_fin.to_csv('total.csv', index=False)
	return df_fin

# Слияние CSV + заполнение базы данных)
def run_inner(date1, date2):	
	df = total(date1, date2)
	cmd = "INSERT INTO articles(code, url, title, authors, abstract, tags, date, src) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"
	for _, row in df.iterrows():
		row_vars = list(row.to_dict().values())
		try:
			apply(cmd, *row_vars)
		except psycopg2.errors.UniqueViolation:
			pass

# Основная "точка входа"
def run(date1, date2):
	start()
	try:
		run_inner(date1, date2)
	finally:
		fin()
