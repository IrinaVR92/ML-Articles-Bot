import logging
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove
import re
from datetime import datetime
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
from inner.csv2db import start, fin, show

from inner.spark_job import run as runSearch

YOUR_TOKEN = open("bot.key").read()

# Включаем логгирование
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Определяем состояния разговора
INIT, DATE, KEYWORDS, SEARCH = range(4)

# Функция для команды /start
def start(update, context):
    reply_keyboard = [['Date', 'Keywords', 'Both', 'Cancel']]
    update.message.reply_text(
        'Hi! I am your article search bot. '
        'Do you want to search by date, keywords or both?',
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True))
    return INIT

from datetime import datetime

# Функция для обработки выбора даты
def init(update, context):
    user_choice = update.message.text
    context.user_data['choice'] = user_choice
    if user_choice == 'Cancel':
        update.message.reply_text('Search cancelled.', reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    if user_choice in ['Date', 'Both']:
        update.message.reply_text('Please enter your date range in DD/MM/YYYY - DD/MM/YYYY format:',
                              reply_markup=ReplyKeyboardRemove())
        return DATE

    if user_choice == 'Keywords':
        update.message.reply_text('Please enter the keywords separated by commas:',
                                  reply_markup=ReplyKeyboardRemove())
        return KEYWORDS

# Функция для обработки выбора даты
def date_choice(update, context):
    date_text = update.message.text.replace(' ', '')
    
    # Проверка формата ввода дат
    try:
        date1_str, date2_str = date_text.split('-')
        date1 = datetime.strptime(date1_str, '%d/%m/%Y')
        date2 = datetime.strptime(date2_str, '%d/%m/%Y')
        if date1 > date2:
            raise ValueError("Start date must be before end date.")

        context.user_data['date1'] = str(date1).split(" ")[0] # date1_str
        context.user_data['date2'] = str(date2).split(" ")[0] # date2_str
        context.user_data['date1str'] = date1_str
        context.user_data['date2str'] = date2_str

    except (ValueError, IndexError):
        update.message.reply_text('Invalid date format. Please enter dates in DD/MM/YYYY - DD/MM/YYYY format:', reply_markup=ReplyKeyboardMarkup([['Cancel']], one_time_keyboard=True))
        return DATE

    if context.user_data.get('choice') == 'Both':
        update.message.reply_text('Please enter the keywords separated by commas:',
                                  reply_markup=ReplyKeyboardRemove())
        return KEYWORDS

    return SEARCH

# Функция для обработки ключевых слов
def keywords_choice(update, context):
    keywords = update.message.text.replace(' ', '').split(',')

    if len(keywords) == 0:
        update.message.reply_text('Invalid keywords format. Please enter the keywords separated by commas:',    reply_markup=ReplyKeyboardMarkup([['Cancel']], one_time_keyboard=True))
        return KEYWORDS

    context.user_data['keywords'] = keywords
        
    return SEARCH

# Функция для форматирования и отправки результатов поиска одним сообщением
def send_search_results(update, context, df):
    messages = []
    current_message = ""

    for index, row in df.iterrows():
        article_info = f"Title: {row['title']}\n" \
                       f"URL: {row['url']}\n" \
                       f"Authors: {row['authors']}\n" \
                       f"Abstract: {row['abstract']}\n\n"

        # Проверяем, не превышает ли длина сообщения максимально допустимый размер
        if len(current_message) + len(article_info) > 4096:
            messages.append(current_message)
            current_message = article_info
        else:
            current_message += article_info

    # Добавляем последнее сообщение, если оно не пустое
    if current_message:
        messages.append(current_message)

    # Отправляем все сформированные сообщения
    for message in messages:
        update.message.reply_text(message)

# Функция для выполнения поиска
def start_search_main(update, context):
    user_choice = context.user_data.get('choice')
    date1 = context.user_data.get('date1')
    date2 = context.user_data.get('date2')
    date1str = context.user_data.get('date1str')
    date2str = context.user_data.get('date2str')
    keywords = context.user_data.get('keywords')

    terms = None
    if keywords:
        terms = ' '.join(keywords)

    user_choice = user_choice.lower()
    if user_choice == 'date':
        result_df = runSearch(date1=date1, date2=date2, terms=None, toCsv=False)
        update.message.reply_text('Searching with dates: {} - {}'.format(date1str, date2str))
    elif user_choice == 'keywords':
        result_df = runSearch(date1=None, date2=None, terms=terms, toCsv=False)
        update.message.reply_text('Searching with keywords: {}'.format(', '.join(keywords)))
    elif user_choice == 'both':
        result_df = runSearch(date1=date1, date2=date2, terms=terms, toCsv=False)
        update.message.reply_text('Searching with dates: {} - {} and keywords: {}'.format(date1str, date2str, ', '.join(keywords)))
    
    if isinstance(result_df, pd.DataFrame) and not result_df.empty:
        send_search_results(update, context, result_df)
    else:
        update.message.reply_text('No results found.')

# Функция для запуска поиска
def start_search(update, context):
    try:
        start_search_main(update, context)
    finally:
        return ConversationHandler.END

# Функция для команды /cancel
def cancel(update, context):
    try:
        update.message.reply_text('Search cancelled.', reply_markup=ReplyKeyboardRemove())
    finally:
        return ConversationHandler.END

# Основная функция для запуска бота
def main():
    # Создаем Updater и передаем ему токен вашего бота
    updater = Updater(YOUR_TOKEN, use_context=True)

    # Получаем диспетчер для регистрации обработчиков
    dp = updater.dispatcher

    # Настройка разговора с состояниями INIT, DATE, KEYWORDS и SEARCH
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            INIT: [MessageHandler(Filters.regex('^(Date|Keywords|Both|Cancel)$'), init)],
            DATE: [MessageHandler(Filters.text & ~Filters.command, date_choice)],
            KEYWORDS: [MessageHandler(Filters.text & ~Filters.command, keywords_choice)],
            SEARCH: [MessageHandler(Filters.text & ~Filters.command, start_search)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Добавляем обработчик разговора в диспетчер
    dp.add_handler(conv_handler)

    # Запускаем бота
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
