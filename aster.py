import telnetlib
import json
import re
import os
import time
from datetime import datetime
import mysql.connector
import logging
from config import (
    HOST,
    PORT,
    USER,
    PASSWORD,
    MYSQL_HOST,
    MYSQL_DB,
    MYSQL_PASSWORD,
    MYSQL_USER,
)
from logging_utils import setup_logging

# Единая настройка логирования для сервиса мониторинга Asterisk.
setup_logging(service_name="asterisk-monitor")


def get_local_time():
    """Получает текущее местное время (UTC+5)"""
    from datetime import datetime, timezone, timedelta

    local_tz = timezone(timedelta(hours=5))
    return datetime.now(local_tz)


def format_local_time(dt=None):
    """Форматирует время в местном формате"""
    if dt is None:
        dt = get_local_time()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def connect_to_asterisk():
    """Подключение к Asterisk AMI"""
    try:
        logging.info(f"🔌 Подключаюсь к Asterisk AMI: {HOST}:{PORT}")
        tn = telnetlib.Telnet(HOST, PORT, timeout=30)
        logging.info("✅ Подключение к Asterisk установлено")

        tn.write("Action: login".encode("ascii") + b"\n")
        username = "Username: " + USER
        tn.write(username.encode("ascii") + b"\n")
        passWord = "Secret: " + PASSWORD
        tn.write(passWord.encode("ascii") + b"\n\n")
        logging.info("🔐 Отправлены учетные данные для входа в AMI")

        return tn
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к Asterisk: {str(e)}")
        return None


def check_connection(tn):
    """Проверка соединения с Asterisk"""
    try:
        # Попытка отправить ping
        tn.write("Action: Ping\n\n".encode("ascii"))
        return True
    except Exception as e:
        logging.warning(f"⚠️ Соединение с Asterisk потеряно: {str(e)}")
        return False


# Инициализация подключения
tn = None
string_NOW = ""
string_out = ""
cd = 0
dict = {}
dictu = {}

# Подключаемся к Asterisk
tn = connect_to_asterisk()
if tn is None:
    logging.error("❌ Не удалось подключиться к Asterisk. Завершение работы.")
    exit(1)


def telnet_for_string(string):
    cd = 0
    global string_out
    for mes in string:
        try:
            # Логируем все события для отладки
            if "Event" in string[mes]:
                logging.info(
                    f"📞 Событие: {string[mes]['Event']} | Context: {string[mes].get('Context', 'N/A')} | Exten: {string[mes].get('Exten', 'N/A')}"
                )

            if (
                string[mes]["Context"] == "freedom_incoming"
                and string[mes]["Event"] == "Newchannel"
                and string[mes]["ChannelStateDesc"] == "Down"
                and string[mes]["Exten"] != "s"
            ):
                Linkedid = string[mes]["Linkedid"]
                dict[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                logging.info(f"📞 Входящий звонок начался: {Linkedid}")

            elif (
                string[mes]["Context"] == "orionit_phones"
                and string[mes]["Event"] == "Newstate"
                and string[mes]["ChannelStateDesc"] == "Up"
                and string[mes]["CallerIDName"] != "<unknown>"
                and string[mes]["Exten"] == "s"
            ):
                Uniqueid = string[mes]["Uniqueid"]
                Linkedid = string[mes]["Linkedid"]
                if Linkedid in dict and Uniqueid != Linkedid:
                    dictu[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                    logging.info(f"📞 Исходящий звонок начался: {Linkedid}")
                elif Linkedid not in dict and Uniqueid != Linkedid:
                    logging.warning(f"⚠️ {Linkedid} - время ответа больше 5 минут")

            elif (
                string[mes]["Event"] == "Hangup"
                and string[mes]["Context"] == "freedom_lua"
                and string[mes]["Exten"] == "h"
            ):
                Uniqueid = string[mes]["Uniqueid"]
                Linkedid = string[mes]["Linkedid"]
                logging.info(f"📞 Звонок завершен: {Linkedid}")

                if Linkedid in dict and Uniqueid == Linkedid:
                    file = datetime.now().strftime("%Y/%m/%d/")
                    call_start_time = datetime.strptime(
                        dict[Linkedid], "%m/%d/%Y, %H:%M:%S"
                    )
                    call_end_time = get_local_time()

                    # Для исходящих звонков используем dictu, для входящих - dict
                    if Linkedid in dictu:
                        # Исходящий звонок
                        answer_time = datetime.strptime(
                            dictu[Linkedid], "%m/%d/%Y, %H:%M:%S"
                        )
                        start = str(answer_time - call_start_time)
                        finish = str(call_end_time - call_start_time)
                        caller_type = "outcoming"
                        logging.info(f"📞 Исходящий звонок завершен: {Linkedid}")
                    else:
                        # Входящий звонок
                        start = "0:00:00"  # Входящие звонки начинаются сразу
                        finish = str(call_end_time - call_start_time)
                        caller_type = "incoming"
                        logging.info(f"📞 Входящий звонок завершен: {Linkedid}")

                    logging.info(f"💾 Записываю звонок в базу: {Linkedid}")
                    logging.info(f"📁 Путь к файлу: {file}")
                    logging.info(
                        f"⏱️ Время начала: {start.split('.')[0]}, окончания: {finish.split('.')[0]}"
                    )

                    cnx = mysql.connector.connect(
                        user=MYSQL_USER,
                        password=MYSQL_PASSWORD,
                        host=MYSQL_HOST,
                        database=MYSQL_DB,
                    )
                    cur = cnx.cursor()
                    sql = "insert into asterisk(id, time_start, time_finish, voip_file, whisper, whisper1, newstatr, newchannel, hangup, stereo, datim, caller) values(%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
                    local_now = get_local_time()
                    val = (
                        Linkedid,
                        start.split(".")[0],
                        finish.split(".")[0],
                        file,
                        False,
                        False,
                        call_start_time.strftime("%H:%M:%S"),
                        call_end_time.strftime("%H:%M:%S"),
                        local_now.strftime("%H:%M:%S"),
                        False,
                        local_now,
                        caller_type,
                    )
                    try:
                        cur.execute(sql, val)
                        cnx.commit()
                        logging.info(
                            f"✅ Звонок {Linkedid} успешно записан в базу данных"
                        )
                    except Exception as e:
                        cnx.rollback()
                        logging.error(f"❌ {Linkedid} - ошибка записи в базу: {str(e)}")
                    cnx.close()

                    # Очищаем только если есть в dictu
                    if Linkedid in dictu:
                        del dictu[Linkedid]
                else:
                    logging.warning(f"⚠️ {Linkedid} - неполные данные для записи в базу")

                # Всегда очищаем dict
                if Linkedid in dict:
                    del dict[Linkedid]

        except UnboundLocalError as er:
            1 + 1
            logging.info(f"-" + str(er))

        except KeyError as er:
            1 + 1
            logging.info(f"-" + str(er))

    if not dict:
        cd += 1
    elif list(dict.keys())[0] not in dictu:
        dict_1 = datetime.strptime(dict[list(dict.keys())[0]], "%m/%d/%Y, %H:%M:%S")
        deletetime = str(datetime.now() - dict_1)
        if int(deletetime.split(":")[1]) > 5:
            del dict[list(dict.keys())[0]]


logging.info("🔄 Начинаю основной цикл мониторинга событий Asterisk...")

last_ping_time = time.time()
PING_INTERVAL = 300  # Ping каждые 5 минут
RECONNECT_DELAY = 10  # Задержка перед переподключением

while True:
    string = ""
    event_string = ""
    elements_string = ""
    c = 0

    try:
        # Проверяем соединение периодически
        current_time = time.time()
        if current_time - last_ping_time > PING_INTERVAL:
            if not check_connection(tn):
                logging.warning("🔄 Попытка переподключения к Asterisk...")
                time.sleep(RECONNECT_DELAY)
                tn = connect_to_asterisk()
                if tn is None:
                    logging.error(
                        "❌ Не удалось переподключиться. Повтор через 30 секунд..."
                    )
                    time.sleep(30)
                    continue
                logging.info("✅ Переподключение к Asterisk успешно")
            last_ping_time = current_time

        read_some = tn.read_some()  # Получаем строчку из AMI

    except Exception as e:
        logging.error(f"❌ Ошибка чтения данных от Asterisk: {str(e)}")
        logging.warning("🔄 Попытка переподключения к Asterisk...")
        time.sleep(RECONNECT_DELAY)
        tn = connect_to_asterisk()
        if tn is None:
            logging.error("❌ Не удалось переподключиться. Повтор через 30 секунд...")
            time.sleep(30)
            continue
        logging.info("✅ Переподключение к Asterisk успешно")
        continue

    string = read_some.decode("utf8", "replace").replace(
        "\r\n", "#"
    )  # Декодируем строчки и заменяем переносы строк на #

    # Отлавливаем начало строки и склеиваем строчку
    if not string.endswith("##"):
        string_NOW = string_NOW + string
        # print('1 --->',string_NOW)

    # Если строчка закончилась, то доклеиваем конец строки и
    # совершаем магию, которая двойной перенос строки в середине строки заменит на $,
    # а все одинарные переносы заменит на #, так-же удалим кавычки и обратные слеши
    if string.endswith("##"):
        string_NOW = string_NOW + string
        string_NOW = string_NOW.replace(
            "##", "$"
        )  # заменяем двойной перенос строки на $
        string_NOW = string_NOW.replace("\n", "#")  # Заменяем  перенос на #
        string_NOW = string_NOW.replace("\r", "#")  # Заменяем  перенос на #
        string_NOW = string_NOW.replace('"', "")  # Удаляем кавычки
        string_NOW = string_NOW.replace("\\", "")  # удаляем обратный слеш

        # Делим полученую строчку на Евенты т.к. двойной перенос как раз её так и делил
        events = re.findall(r"[A-Z][\w]+:\s[^$]+", string_NOW)
        for event in events:
            c += 1

            event_elements = re.findall(
                r"[A-Z][\w]+:\s[^#]+", event
            )  # А тут делим евенты на елемены
            for element in event_elements:
                element = (
                    '"' + element.replace(": ", '": "') + '", '
                )  # Вручную делаем словарь
                elements_string = (
                    elements_string + element
                )  # Склеиваем строчки обратно, получаем словарь

            # собираем обратно евенты попутно формирую json:
            event_string = (
                event_string + '"' + str(c) + '": ' + "{" + elements_string + "}"
            )
            event_string = event_string.replace(
                "}{", "},{"
            )  #   Добавляем запятую между евентами
            event_string = event_string.replace(", }", "}, ")  #
        event_string = "{" + event_string + "}"
        event_string = event_string.replace("}, }", "}}")

        # Превращаем полученую строчку в json, если вдруг есть ошибка в синтаксисе json, то выводим как сам невалидный
        # json, так и строчку  из которой не получилось его собрать.
        try:
            parsed_string = json.loads(event_string)
        except json.decoder.JSONDecodeError:
            logging.error(
                f"#############################################",
                "\n" + event_string,
                "\n" + string_NOW,
                "\n" + "#############################################",
                "\n",
            )

        # Отправляем полученую строчку в функуию "telnet_for_string", в которой уже можно обработать полученую строчку.
        telnet_for_string(parsed_string)
        string_NOW = ""  # Очищем строчку
