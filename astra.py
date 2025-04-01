import telnetlib
import json
import re
from datetime import datetime
import logging
import mysql.connector
from config import HOST, PORT, USER, PASSWORD, MYSQL_HOST, MYSQL_DB, MYSQL_PASSWORD, MYSQL_USER

log_file = '/opt/python/logs/aster.log'
#log_file = 'C:/workspace/93/aster.log'
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
ishod = {}
ishodu = {}
filetimeIN = {}
filetimeOUT = {}

tn.write(passWord.encode('ascii') + b"\n\n")

def telnet_for_string(string):
    cd = 0
    global string_out
    for mes in string:
        try:
            if string[mes]['Context'] == 'freedom_incoming' and string[mes]['Event'] == 'Newchannel' and string[mes]['ChannelStateDesc'] == 'Down':
                if string[mes]['Exten'] != 's': 
                    Linkedid = string[mes]['Linkedid']
                    dict[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                    filetimeIN[Linkedid] = datetime.now().strftime('%Y/%m/%d/')
                    #print(f'входящий Newchannel {Linkedid}')
                elif string[mes]['Exten'] == 's':
                    Linkedid = string[mes]['Linkedid']
                    ishod[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                    filetimeOUT[Linkedid] = datetime.now().strftime('%Y/%m/%d/')
                    #print(f'исходящий Newchannel {Linkedid}')

            elif string[mes]['Event'] == 'Newstate':
                if string[mes]['ChannelStateDesc'] == 'Up' and string[mes]['CallerIDName'] != '<unknown>' and string[mes]['Exten'] == 's' and string[mes]['Context'] == 'orionit_phones':
                    Uniqueid = string[mes]['Uniqueid']
                    Linkedid = string[mes]['Linkedid']
                    if Linkedid in dict and Uniqueid != Linkedid:
                        dictu[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                        #print(f'входящий Newstate {Linkedid}')
                    elif Linkedid not in dict and Uniqueid != Linkedid:
                        logging.warning(f'******' + Linkedid + 'время ответа больше 5 минут')
                elif string[mes]['Context'] == 'freedom_incoming' and  string[mes]['ChannelStateDesc'] == 'Up'  and len(string[mes]['Exten']) == 11:
                    Linkedid = string[mes]['Linkedid']
                    if Linkedid in ishod:
                        ishodu[Linkedid] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                        #print(f'исходящий Newstate {Linkedid}')

            elif string[mes]['Event'] == 'Hangup':
                if string[mes]['Context'] == 'freedom_lua' and string[mes]['Exten'] == 'h':               
                    Uniqueid = string[mes]['Uniqueid']
                    Linkedid = string[mes]['Linkedid']
                    if Linkedid in dict and Linkedid in dictu and Uniqueid == Linkedid:
                        file = filetimeIN[Linkedid]
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
                        #print(f'входящий Hangup {Linkedid}')
                        #print(file)
                        #print(datetime.now())
                        try:
                            #print(sql, val)
                            cur.execute(sql, val) 
                            cnx.commit()
                            #print(f'записалось incoming {Linkedid}\n')
                        except Exception as e:
                            cnx.rollback()
                            logging.error(f'----' + Linkedid + 'не записалось в базу (входящий)' + str(e))
                            #print(f'незаписалось incoming {Linkedid}\'n')
                        cnx.close()
                        del dictu[Linkedid] 
                    del dict[Linkedid]
                    del filetimeIN[Linkedid]
                elif string[mes]['Context'] == 'freedom_incoming' and string[mes]['ChannelStateDesc'] == 'Up':
                    Linkedid = string[mes]['Linkedid']
                    if Linkedid in ishod and Linkedid in ishodu:
                        file =  filetimeOUT[Linkedid]
                        b =  datetime.strptime(ishod[Linkedid], '%m/%d/%Y, %H:%M:%S')
                        a = datetime.strptime(ishodu[Linkedid], '%m/%d/%Y, %H:%M:%S')
                        start = str(datetime.strptime(ishodu[Linkedid], '%m/%d/%Y, %H:%M:%S') - datetime.strptime(ishod[Linkedid], '%m/%d/%Y, %H:%M:%S'))
                        finish = str(datetime.now() - datetime.strptime(ishod[Linkedid], '%m/%d/%Y, %H:%M:%S'))
                        cnx = mysql.connector.connect(user=MYSQL_USER, 
                                                      password=MYSQL_PASSWORD,
                                                      host=MYSQL_HOST,
                                                      database=MYSQL_DB)
                        cur = cnx.cursor() 
                        sql = "insert into asterisk(id, time_start, time_finish, voip_file, whisper, whisper1, newstatr, newchannel, hangup, stereo, datim, caller) values(%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
                        val = (Linkedid, start.split(".")[0], finish.split(".")[0], file, False, False, a.strftime('%H:%M:%S'), b.strftime('%H:%M:%S'), datetime.now().strftime('%H:%M:%S'), False, datetime.now(), 'outcoming')
                        #print(file)
                        #print(datetime.now())

                        try:
                            #print(sql, val)
                            cur.execute(sql, val)
                            cnx.commit()
                            #print(f'записалось outcoming {Linkedid}\n')
                        except Exception as e:
                            cnx.rollback()
                            logging.error(f'----' + Linkedid + 'не записалось в базу (исходящий)' + str(e))
                           # print(f'незаписалось outcoming {Linkedid}\'n')
                        cnx.close()
                        del ishodu[Linkedid] 
                    del ishod[Linkedid]
                    del filetimeOUT[Linkedid]


        
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
            del filetimeIN[list(filetimeIN.keys())[0]]

    if not ishod:
        cd += 1
    elif list(ishod.keys())[0] not in ishodu:
        ishod_1 = datetime.strptime(ishod[list(ishod.keys())[0]], '%m/%d/%Y, %H:%M:%S')
        deletetime = str(datetime.now() - ishod_1)
        if int(deletetime.split(':')[1]) > 5:
            del ishod[list(ishod.keys())[0]]
            del filetimeOUT[list(filetimeOUT.keys())[0]]

while True:
    string = ''
    event_string = ''
    elements_string = ''
    c = 0

    read_some = tn.read_some()
    string = read_some.decode('utf8', 'replace').replace('\r\n', '#') 

    if not string.endswith('##'):
        string_NOW = string_NOW + string

    if string.endswith('##'):
        string_NOW = string_NOW + string
        string_NOW = string_NOW.replace('##', '$')  
        string_NOW = string_NOW.replace('\n', '#') 
        string_NOW = string_NOW.replace('\r', '#')  
        string_NOW = string_NOW.replace('"', '')  
        string_NOW = string_NOW.replace('\\', '')   

        events = re.findall(r'[A-Z][\w]+:\s[^$]+', string_NOW)
        for event in events:
            c+=1

            event_elements = re.findall(r'[A-Z][\w]+:\s[^#]+', event) 
            for element in event_elements:
                element = '\"' + element.replace(': ', '\": "') + '\", '
                elements_string = elements_string + element

            event_string = event_string + '\"' + str(c) + '\": ' + '{' + elements_string + '}'
            event_string = event_string.replace('}{', '},{')   
            event_string = event_string.replace(', }', '}, ')   
        event_string = '{' + event_string + '}'
        event_string = event_string.replace('}, }', '}}')

        try:
            parsed_string = json.loads(event_string)
        except json.decoder.JSONDecodeError:
            logging.error(f'#############################################', '\n' + event_string, '\n' + string_NOW, '\n' + '#############################################', '\n')

        telnet_for_string(parsed_string)
        string_NOW = '' 
