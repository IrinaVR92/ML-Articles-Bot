# Этот модуль загружает данные из CWP в CSV-файл

import pandas as pd
from time import sleep
from os import system
from selenium import webdriver
from selenium.webdriver.common.by import By
import calendar

# Соотношение названий месяцев и их номеров для раскодирования (по 3 буквы)
MONTHS = {calendar.month_name[i][:3]: str(i) for i in range(1, 13)}    

# Требуемые адреса для Selenium
BASE_URL = "https://paperswithcode.com"
PAPER_SUBURL = "/paper/"
PAPER_URL = f"{BASE_URL}{PAPER_SUBURL}"
PAPER_SKIP = len(PAPER_URL)

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

# Класс для более простого управления экземпляром Selenium'а.

# Для работы Selenium необходимо установить его драйвер под конкретный
# браузер и его версию.

# Часто драйвер Selenium не закрывается, поэтому необходимо уничтожать
# его "вручную" (через консольную команду).

# В идеале доделать и сделать полностью безголовым (сейчас открывается
# в видимом окне браузера).

# Команда для уничтожения Selenium "вручную" написана под Windows,
# под Linux она будет какой-то другой.

class Selenium:
    
    driver = None
    
    def __init__(self):
        raise Exception("This class can not have an instance")

    # Создаем управляемую вкладку
    @classmethod
    def start(cls, url="about:blank"):        
        cls.stop()
        cls.driver = webdriver.Chrome()
        cls.nav(url)
    
    # Переход на страницу    
    @classmethod
    def nav(cls, url):
        if cls.driver is None:
            return
        cls.driver.get(url)
    
    # Доступ к драйверу Selenium напрямую
    @classmethod
    def get(cls):
        return cls.driver
    
    # Остановка Selenium
    @classmethod
    def stop(cls):
        if cls.driver is not None:
            cls.driver.close()
            cls.driver = None

# Остановка/уничтожение "вручную" (полезно делать время от времени)
def purge_selenium():
	system('taskkill /im chromedriver.exe /f')

### Блок команд JavaScript

# Команды для получения данных со страниц со статьями:
# заголовок, авторы и дата, теги, содержимое
JS_COMMANDS = [
    """return document.querySelector(".paper-title h1").innerText""",
    """return [...document.querySelector(".paper-title div.authors").querySelectorAll(".author-span")].map(e => e.innerText.trim())""",
    """return [...document.querySelector(".paper-tasks").querySelectorAll(".badge-primary")].map(e => e.innerText.trim())""",
    """return document.querySelector(".paper-abstract p").innerText""",
]

# Команда для управления циклом прокрутки (когда стоит остановиться)
js_enough = '''return document.querySelector("a.infinite-more-link") === null'''

# Команда для получения всех элементов, содержащих статьи
js_items = '''
    const place = document.querySelector("div.infinite-container");
    const buf = [];
    place.querySelectorAll("div.infinite-item").forEach(item => {
        buf.push(item.querySelector("h1 > a").getAttribute('href'));
    });
    return buf;
'''

### Блок команд JavaScript закончен

# Прокрутка при первой загрузке, чтобы отобразить все статьи (их оказалось
# немного на этом ресурсе, и, увы, не удалось их упорядочить по времени):
def first_run(driver):
	while True:
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		sleep(0.25) # чтобы не спамить запросами
		if driver.execute_script(js_enough):
			break

# Выцепляем данные со страниц, соответствующих найденным элементам
def gather_metapapers(driver):
	paths = driver.execute_script(js_items)
	papers = [f"{BASE_URL}{p}" for p in paths if p.startswith(PAPER_SUBURL)]
	meta_papers = []

	for paper_url in papers:
		driver.get(paper_url)
		paper_code = paper_url[PAPER_SKIP:]
		paper_title, paper_authors, paper_tags, paper_abstract = [driver.execute_script(command) for command in JS_COMMANDS]
		meta_papers.append([paper_code, paper_url, paper_title, paper_authors, paper_abstract, paper_tags])
		sleep(0.25)
	
	return meta_papers

# Обрабатываем и довычищаем полученные данные
def proceed(meta_paper):
    authors = meta_paper[3]
    tags = meta_paper[-1]
    new_authors = '; '.join(authors[1:])
    new_tags = '; '.join(tags)
    # Первый "автор" обычно дата публикации,
    # другие единичные случаи не рассматриваем
    publ_date = authors[0].split()
    publ_date[1] = MONTHS.get(publ_date[1], publ_date[1])
    publ_date = '/'.join(publ_date)
    return [
        *meta_paper[0:3],        
        new_authors, 
        *meta_paper[4:-1],
        new_tags,
        publ_date
    ]

# Основная "точка входа"
def run():
	Selenium.start(BASE_URL)
	driver = Selenium.get()
	first_run(driver)
	meta_papers = gather_metapapers(driver)
	correct_papers = [proceed(meta_paper) for meta_paper in meta_papers]
	df = pd.DataFrame(correct_papers, columns=ARXIV_COLS)
	df.to_csv("cwp.csv", index=False)
	Selenium.stop()
