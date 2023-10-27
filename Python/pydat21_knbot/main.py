# ВШЭ, ФКН, Программа "Специалист по DS" (2021/2022), Курс "Программирование на Python"
# Проект телеграм-бота для скачивания котировок по тикерам Московской Биржи
# Выполнил Кирилл Н., 2021 г.

import telebot
import requests
import pandas as pd
import apimoex
from datetime import datetime, timedelta
from threading import Thread
import time
import json

token_file = open('token.txt')
token = token_file.read().strip()
bot = telebot.TeleBot(token)

tickers = []
user_param = {}
user_param_json_file_name = 'app.json'
quotes_fetched = False
start_date = '2021-01-01'
end_date = datetime.now()
users2spam = []
spam_freq = 5


@bot.message_handler(commands=['start'])
def command_start(message):
    global tickers
    global user_param
    global user_param_json_file_name

    text = "Добрый день! Это телеграм-бот pydat21_knbot. " \
            "Я умею работать с API МосБиржи. По названиям тикеров я могу скачивать цены закрытия с начала 2021 г. " \
            "Также я могу рассылать с заданной периодичностью последние цены для всех выбранных тикеров. " \
            "Чтобы посмотреть полный список команд, нажмите /help. "
    bot.send_message(message.from_user.id, text)
    try:
        with open(user_param_json_file_name, "r") as user_param_json_file:
            user_param = json.load(user_param_json_file)
            tickers = user_param[str(message.from_user.id)]
    except:
        tickers = ['ROSN', 'GAZP']  # Тикеры по умолчанию


@bot.message_handler(commands=['help'])
def command_help(message):
    text = "Список команд:\n" \
            "/get_data - запустить сбор данных;\n" \
            "/tickers - вывести выбранные тикеры;\n" \
            "/add <список тикеров через пробел> - добавить введенные тикеры;\n" \
            "/del <список тикеров через пробел> - удалить введенные тикеры;\n" \
            "/get_file - получить csv-файл с данными;\n" \
            "/mean - вывести среднюю цену закрытия;\n" \
            "/median - вывести медианную цену закрытия;\n" \
            "/date <дата в формате yyyy-mm-dd> - вывести цену закрытия на введенную дату;\n" \
            "/start_spam - разрешить рассылку последних котировок;\n" \
            "/stop_spam - запретить рассылку последних котировок.\n"
    bot.send_message(message.from_user.id, text)


@bot.message_handler(commands=['get_data'])
def command_get_data(message):
    global quotes_fetched
    global tickers

    quotes_fetched = False
    if len(tickers) == 0:
        bot.send_message(message.from_user.id, "Сначала добавьте тикеры с помощью команды /add.")
        return
    bot.send_message(message.from_user.id, "Начинаю сбор данных. Подождите..")
    session = requests.Session()

    data = apimoex.get_board_history(session, tickers[0], start_date, end_date, ('TRADEDATE', 'CLOSE'))
    res_df = pd.DataFrame(data)
    res_df.set_index('TRADEDATE', inplace=True)
    res_df['TICKER'] = tickers[0]

    for ticker in tickers[1:]:
        data = apimoex.get_board_history(session, ticker, start_date, end_date, ('TRADEDATE', 'CLOSE'))
        df = pd.DataFrame(data)
        df.set_index('TRADEDATE', inplace=True)
        df['TICKER'] = ticker
        res_df = pd.concat([res_df, df], ignore_index=False)

    res_df.to_csv('data.csv', sep=';', encoding='utf-8')
    quotes_fetched = True
    bot.send_message(message.from_user.id, "Данные собраны, вы можете получить файл с помощью команды /get_file.")


@bot.message_handler(commands=['tickers'])
def command_tickers(message):
    global tickers

    if len(tickers) == 0:
        text = "Список тикеров пока пуст, вы можете добавить их с помощью команды /add, " \
                "либо загрузите сохраненный список или список по умолчанию с помощью команды /start."
        bot.send_message(message.from_user.id, text)
    else:
        bot.send_message(message.from_user.id, f"Выбранные тикеры:\n{' '.join(tickers)}")


@bot.message_handler(commands=['add'])
def command_add_tickers(message):
    global tickers
    global user_param
    global user_param_json_file_name
    global quotes_fetched

    params = message.text.split()
    if len(params) == 1:
        text = "Данная команда должна вводиться вместе с тикерами через пробел, попробуйте еще раз.\n"
        bot.send_message(message.from_user.id, text)
        return
    for param in params[1:]:
        param = param.upper()
        if param in tickers:
            bot.send_message(message.from_user.id, "Тикер {} уже есть в списке.\n".format(param))
        elif len(apimoex.find_security_description(requests.Session(), param)) == 0:
            bot.send_message(message.from_user.id, "Тикера {} нет на МосБирже.\n".format(param))
        else:
            tickers.append(param)
            user_param[str(message.from_user.id)] = tickers
            with open(user_param_json_file_name, 'w') as user_param_json_file:
                json.dump(user_param, user_param_json_file)
                user_param_json_file.close()
            if quotes_fetched:
                quotes_fetched = False
            bot.send_message(message.from_user.id, "Тикер {} успешно добавлен.\n".format(param))


@bot.message_handler(commands=['del'])
def command_del_ticker(message):
    global tickers
    global user_param
    global user_param_json_file_name

    params = message.text.split()
    if len(params) == 1:
        text = "Данная команда должна вводиться вместе с тикерами через пробел, попробуйте еще раз.\n"
        bot.send_message(message.from_user.id, text)
        return
    for param in params[1:]:
        param = param.upper()
        if param not in tickers:
            bot.send_message(message.from_user.id, "Тикера {} нет в списке.\n".format(param))
        else:
            tickers.pop(tickers.index(param))
            user_param[str(message.from_user.id)] = tickers
            with open(user_param_json_file_name, 'w') as user_param_json_file:
                json.dump(user_param, user_param_json_file)
                user_param_json_file.close()
            bot.send_message(message.from_user.id, "Тикер {} успешно удален.\n".format(param))


@bot.message_handler(commands=['get_file'])
def command_get_file(message):
    global quotes_fetched
    if quotes_fetched:
        data_file = open('data.csv', 'rb')
        bot.send_document(message.from_user.id, data_file)
        data_file.close()
    else:
        bot.send_message(message.from_user.id, "Данные еще не собраны. Введите команду /get_data, чтобы это сделать.")


@bot.message_handler(commands=['mean'])
def command_mean(message):
    global quotes_fetched
    global tickers
    global start_date
    global end_date

    if quotes_fetched:
        bot.send_message(message.from_user.id, "Средние котировки за период с {} по {}:\n".\
                         format(start_date, end_date.date()))
        data = pd.read_csv('data.csv', sep=';', encoding='utf-8')
        for ticker in tickers:
            mean = float(data[data['TICKER'] == ticker].mean())
            bot.send_message(message.from_user.id, "{} - {:.2f}\n".format(ticker, mean))
    else:
        bot.send_message(message.from_user.id, "Данные еще не собраны. Введите команду /get_data, чтобы это сделать.")


@bot.message_handler(commands=['date'])
def command_date(message):
    global quotes_fetched
    global tickers

    params = message.text.split()
    if len(params) != 2:
        bot.send_message(message.from_user.id, "Данная команда должна вводиться вместе с датой через пробел \
            в формате yyyy-mm-dd, попробуйте еще раз.")
        return
    try:
        check_date = datetime.strptime(params[1], "%Y-%m-%d").date()
    except:
        bot.send_message(message.from_user.id, "Неверная дата, попробуйте еще раз.")
        return

    if quotes_fetched:
        bot.send_message(message.from_user.id, "Цены закрытия на {}:\n".format(check_date))
        data = pd.read_csv('data.csv', sep=';', encoding='utf-8')
        tickers_counter = 0
        check_date = str(check_date)
        for ticker in tickers:
            try:
                price = data.loc[data['TRADEDATE'] == check_date, 'CLOSE'].values[tickers_counter]
                bot.send_message(message.from_user.id, "{} - {:.2f}\n".format(ticker, price))
            except:
                bot.send_message(message.from_user.id, "Для {} нет цены на такую дату.".format(ticker))

            tickers_counter += 1
    else:
        bot.send_message(message.from_user.id, "Данные еще не собраны. Введите команду /get_data, чтобы это сделать.")


@bot.message_handler(commands=['median'])
def command_median(message):
    global quotes_fetched
    global tickers
    global start_date
    global end_date

    if quotes_fetched:
        bot.send_message(message.from_user.id, "Медианные котировки за период с {} по {}:\n".\
                         format(start_date, end_date.date()))
        data = pd.read_csv('data.csv', sep=';', encoding='utf-8')
        for ticker in tickers:
            mean = float(data[data['TICKER'] == ticker].median())
            bot.send_message(message.from_user.id, "{} - {:.2f}\n".format(ticker, mean))
    else:
        bot.send_message(message.from_user.id, "Данные еще не собраны. Введите команду /get_data, чтобы это сделать.")


@bot.message_handler(commands=['start_spam'])
def command_start_spam(message):
    global users2spam
    global spam_freq

    if message.from_user.id not in users2spam:
        users2spam.append(message.from_user.id)
        text = "Вы успешно добавлены в рассылку (периодичность - " + str(spam_freq) + " сек). " \
                "Чтобы отписаться, введите команду /stop_spam."
        bot.send_message(message.from_user.id, text)


@bot.message_handler(commands=['stop_spam'])
def command_stop_spam(message):
    global users2spam
    users2spam.remove(message.from_user.id)
    bot.send_message(message.from_user.id, "Вы успешно удалены из рассылки. Чтобы подписаться, введите команду /start_spam.")


@bot.message_handler(content_types=['text'])
def command_text(message):
    bot.send_message(message.from_user.id, "Команда не распознана.")


def spam():
    global users2spam
    global tickers
    global spam_freq

    session = requests.Session()
    while True:
        date2 = datetime.now().strftime("%Y-%m-%d")
        date1 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        for ticker in tickers:
            data = apimoex.get_board_history(session, ticker, date1, date2, ('TRADEDATE', 'CLOSE'))
            df = pd.DataFrame(data)
            price = df.tail(1)['CLOSE'].values[0]
            for user2spam in users2spam:
                bot.send_message(user2spam, "Последняя цена {} - {}".format(ticker, price))
        time.sleep(spam_freq)


def polling():
    bot.polling(none_stop=True)


polling_thread = Thread(target=polling)
spam_thread = Thread(target=spam)

polling_thread.start()
spam_thread.start()
