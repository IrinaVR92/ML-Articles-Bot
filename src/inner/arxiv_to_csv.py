# Этот модуль загружает данные из Arxiv в CSV-файл

import calendar
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from time import sleep

# Соотношение названий месяцев и их номеров для раскодирования
MONTHS = {calendar.month_name[i]: str(i) for i in range(1, 13)}

# Максимальное количество статей на странице
ARXIV_PAGESIZE = 200

# Параметры расширенного поиска
ARXIV_PARAMS = {
    "advanced": "1",
    "terms-0-operator": "AND",
    "terms-0-field": "title",
    "classification-computer_science": "y",
    "classification-include_cross_list": "include",
    "date-filter_by": "date_range",
    "date-from_date": "#1",
    "date-to_date": "#2",
    "date-date_type": "submitted_date",
    "abstracts": "show",
    "size": str(ARXIV_PAGESIZE),
    "order": "-announced_date_first",
    "start": "#3"
}

# Колонки для CSV
ARXIV_COLS = [
    "code",
    "url",
    "title",
    "authors",
    "abstract",
    "tags",
    "date"
]

# Генерируем URL с подходящими параметрами
def get_url(date1, date2, start=0):
    ARXIV_PARAMS['date-from_date'] = date1
    ARXIV_PARAMS['date-to_date'] = date2
    ARXIV_PARAMS['start'] = str(start)
    params = "&".join(("=".join((i, j)) for i, j in ARXIV_PARAMS.items()))
    url = f"https://arxiv.org/search/advanced?{params}"
    return url

# Извлекаем количество статей из фразы в духе "N of M"
def get_item_count(b):
    attempt = [''.join([l for l in term.strip() if l.isdigit()]) for term in b.find("h1", class_="title").text.strip().split("of")][-1]
    return int(attempt) if attempt else 0

# Вычисляем количество страниц с результатами
def get_page_count(n):
    return max(1, ((n - 1) // ARXIV_PAGESIZE) + 1)

# Компоненты для CSV: авторы, текст, название, код и URL, теги, дата публикации

def authors(elem):
    return '; '.join([i.strip(',') for i in [i.strip() for i in elem.find("p", class_="authors").text.split("\n")] if i and not i.endswith(':')])

def abstract(elem):
    return elem.find("span", class_="abstract-full").contents[0].strip()

def title(elem):
    return elem.find("p", class_="title").text.strip()

def url_code(elem):
    link = elem.find("p", class_="list-title").find("a")
    return link["href"], link.text

def tags(elem):
    return '; '.join([e["data-tooltip"] for e in elem.find("div", class_="tags").find_all("span", class_="tag")])

def publ_date(elem):
    publ = elem.find("p", class_="is-size-7").contents[1].strip()
    for m in MONTHS:
        if m in publ:
            publ = publ.replace(m, MONTHS[m], 1)
            break
    return '/'.join([i.strip(",;") for i in publ.split() if i])

# Процесс захвата нужных статей
def gather(date1, date2):
	# Смещение по количеству статей
	start = 0
	# Смещение по количеству страниц
	page = 1
	# Общее количество статей/страниц
	cnt, pgs = None, None
	# Захваченные результаты
	strs = []

	while True:
		url = get_url(date1, date2, start)    
		r = requests.get(url)
		b = bs(r.text)

		if cnt is None:
			cnt = get_item_count(b)
		if pgs is None:
			pgs = get_page_count(cnt)
			
		# Почему-то не отобразилось количество, это фатальная ошибка
		if not cnt:
			break
			
		# Все результаты на странице
		total = b.find_all("li", class_="arxiv-result")

		for elem in total:
			ur, co = url_code(elem)
			ti = title(elem)
			au = authors(elem)
			ab = abstract(elem)
			ta = tags(elem)
			pu = publ_date(elem)
			strs.append([co, ur, ti, au, ab, ta, pu])

		if page >= pgs:
			break
		
		page += 1
		start += ARXIV_PAGESIZE
		sleep(0.3)
		
	return strs
    
# Основная "точка входа"
def run(date1, date2):
	strs = gather(date1, date2)
	df = pd.DataFrame(strs, columns=ARXIV_COLS).drop_duplicates()
	df.to_csv("arxiv.csv", index=False)
