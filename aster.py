import telnetlib
import json
import re
from datetime import datetime
import mysql.connector
import logging
from config import HOST, PORT, USER, PASSWORD, MYSQL_HOST, MYSQL_DB, MYSQL_PASSWORD, MYSQL_USER

log_file = '/opt/python/logs/aster.log'
logging.basicConfig(filename=log_file, level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

tn = telnetlib.Telnet(HOST,PORT)
tn.write("Action: login".encode('ascii') + b"\n")
username = "Username: " + USER
tn.write(username.encode('ascii') + b"\n")
passWord = "Secret: " + PASSWORD
string_NOW = ''
string_out = ''
cd = 0
dict = {}
dictu = {}

tn.write(passWord.encode('ascii') + b"\n\n")

def telnet_for_string(string):
    cd = 0
    global string_out
    for mes in string:
        try:
            if string[mes]['Context'] == 'freedom_incoming' and string[mes]['Event'] == 'Newchannel' and string[mes]['ChannelStateDesc'] == 'Down' and string[mes]['Exten'] != 's': 
                Linkedid = string[mes]['Linkedid']
                dict[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            
            elif string[mes]['Context'] == 'orionit_phones' and string[mes]['Event'] == 'Newstate' and string[mes]['ChannelStateDesc'] == 'Up' and string[mes]['CallerIDName'] != '<unknown>' and string[mes]['Exten'] == 's':
                Uniqueid = string[mes]['Uniqueid']
                Linkedid = string[mes]['Linkedid']
                if Linkedid in dict and Uniqueid != Linkedid:
                    dictu[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                elif Linkedid not in dict and Uniqueid != Linkedid:
                    logging.warning(f'******' + Linkedid + 'время ответа больше 5 минут')

            elif string[mes]['Event'] == 'Hangup' and string[mes]['Context'] == 'freedom_lua' and string[mes]['Exten'] == 'h':               
                Uniqueid = string[mes]['Uniqueid']
                Linkedid = string[mes]['Linkedid']
                if Linkedid in dict and Linkedid in dictu and Uniqueid == Linkedid:
                    file =  datetime.now().strftime('%Y/%m/%d/')
                    b =  datetime.strptime(dict[Linkedid], '%m/%d/%Y, %H:%M:%S')
                    a = datetime.strptime(dictu[Linkedid], '%m/%d/%Y, %H:%M:%S')
                    start = str(datetime.strptime(dictu[Linkedid], '%m/%d/%Y, %H:%M:%S') - datetime.strptime(dict[Linkedid], '%m/%d/%Y, %H:%M:%S'))
                    finish = str(datetime.now() - datetime.strptime(dict[Linkedid], '%m/%d/%Y, %H:%M:%S'))
                    cnx = mysql.connector.connect(user=MYSQL_USER, 
                                                  password=MYSQL_PASSWORD,
                                                  host=MYSQL_HOST,
                                                  database=MYSQL_DB)
                    cur = cnx.cursor() 
                    sql = "insert into asterisk(id, time_start, time_finish, voip_file, whisper, whisper1, newstatr, newchannel, hangup, stereo, datim, caller) values(%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
                    val = (Linkedid, start.split(".")[0], finish.split(".")[0], file, False, False, a.strftime('%H:%M:%S'), b.strftime('%H:%M:%S'), datetime.now().strftime('%H:%M:%S'), False, datetime.now(), 'incoming')
                    try:
                        cur.execute(sql, val)
                        cnx.commit()
                    except Exception as e:
                        cnx.rollback()
                        logging.error(f'----' + Linkedid + 'не записалось в базу ' + str(e))
                    cnx.close()
                    del dictu[Linkedid] 
                del dict[Linkedid]
        
        except UnboundLocalError as er:
            1+1
            logging.info(f'-' + str(er))
        
        except KeyError as er:
            1+1
            logging.info(f'-' + str(er))
    
    if not dict:
        cd += 1
    elif list(dict.keys())[0] not in dictu:
        dict_1 = datetime.strptime(dict[list(dict.keys())[0]], '%m/%d/%Y, %H:%M:%S')
        deletetime = str(datetime.now() - dict_1)
        if int(deletetime.split(':')[1]) > 5:
            del dict[list(dict.keys())[0]]

while True:
    string = ''
    event_string = ''
    elements_string = ''
    c = 0

    read_some = tn.read_some()  # Получаем строчку из AMI
    
    string = read_some.decode('utf8', 'replace').replace('\r\n', '#')   # Декодируем строчки и заменяем переносы строк на #

    # Отлавливаем начало строки и склеиваем строчку
    if not string.endswith('##'):
        string_NOW = string_NOW + string
        # print('1 --->',string_NOW)

    # Если строчка закончилась, то доклеиваем конец строки и
    # совершаем магию, которая двойной перенос строки в середине строки заменит на $,
    # а все одинарные переносы заменит на #, так-же удалим кавычки и обратные слеши
    if string.endswith('##'):
        string_NOW = string_NOW + string
        string_NOW = string_NOW.replace('##', '$')  # заменяем двойной перенос строки на $
        string_NOW = string_NOW.replace('\n', '#')  # Заменяем  перенос на #
        string_NOW = string_NOW.replace('\r', '#')  # Заменяем  перенос на #
        string_NOW = string_NOW.replace('"', '')    # Удаляем кавычки
        string_NOW = string_NOW.replace('\\', '')   # удаляем обратный слеш

        # Делим полученую строчку на Евенты т.к. двойной перенос как раз её так и делил
        events = re.findall(r'[A-Z][\w]+:\s[^$]+', string_NOW)
        for event in events:
            c+=1

            event_elements = re.findall(r'[A-Z][\w]+:\s[^#]+', event)   # А тут делим евенты на елемены
            for element in event_elements:
                element = '\"' + element.replace(': ', '\": "') + '\", '# Вручную делаем словарь
                elements_string = elements_string + element # Склеиваем строчки обратно, получаем словарь

            # собираем обратно евенты попутно формирую json:
            event_string = event_string + '\"' + str(c) + '\": ' + '{' + elements_string + '}'
            event_string = event_string.replace('}{', '},{')    #   Добавляем запятую между евентами
            event_string = event_string.replace(', }', '}, ')   #
        event_string = '{' + event_string + '}'
        event_string = event_string.replace('}, }', '}}')

        # Превращаем полученую строчку в json, если вдруг есть ошибка в синтаксисе json, то выводим как сам невалидный
        # json, так и строчку  из которой не получилось его собрать.
        try:
            parsed_string = json.loads(event_string)
        except json.decoder.JSONDecodeError:
            logging.error(f'#############################################', '\n' + event_string, '\n' + string_NOW, '\n' + '#############################################', '\n')

        # Отправляем полученую строчку в функуию "telnet_for_string", в которой уже можно обработать полученую строчку.
        telnet_for_string(parsed_string)
        string_NOW = '' # Очищем строчку