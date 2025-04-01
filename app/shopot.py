import mysql.connector
import wave
import struct
import pysftp
import os
import whisperx
import logging
import time
from datetime import datetime
from config import MYSQL_HOST, MYSQL_DB, MYSQL_PASSWORD, MYSQL_USER, SFTP_HOST, SFTP_PASSWORD, SFTP_USER

#log_file = 'C:/workspace/93/whisp.log'
log_file = '/opt/python/logs/whisp.log'
logging.basicConfig(filename=log_file, level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

device = 'cuda'
batch_size = 4
#compute_type = 'float16'
compute_type = 'int8'
model = whisperx.load_model("large-v3", device, compute_type=compute_type, language='ru')

def to_string(segment):
    text = ''
    for seg in segment:
        text += str(seg['text'])
    return text

def channels_whis(nn, id, caller):
    sss = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                            host=MYSQL_HOST,
                            database=MYSQL_DB)
    lin = int(nn.split('_')[0])
    audio = whisperx.load_audio(nn)
    resu = model.transcribe(audio, batch_size=batch_size, language='ru')
    sqlone = sss.cursor()
    if (lin == 1 and caller == 'incoming') or (lin == 2 and caller == 'outcoming'):
        sqlone.execute('UPDATE whisper SET employee = %s WHERE id = %s', (to_string(resu['segments']), id))
        sss.commit()
    elif (lin == 2 and caller == 'incoming') or (lin == 1 and caller == 'outcoming'):
        sqlone.execute('UPDATE whisper SET friend = %s WHERE id = %s', (to_string(resu['segments']), id))
        sss.commit()
    os.remove(nn)
    sss.close()

def create_file_one_channel(name, framerate, sampwidth, channels, values_copy):
    out_file = wave.open(name, "wb")
    out_file.setframerate(framerate)
    out_file.setsampwidth(sampwidth)
    out_file.setnchannels(channels)
    audio_data = struct.pack(f"<{len(values_copy)}h", *values_copy)
    out_file.writeframes(audio_data)
    out_file.close()

while True:
    to_whisper = True
    to_stereo = True
    cnx = mysql.connector.connect(user=MYSQL_USER, 
                                  password=MYSQL_PASSWORD,
                                  database=MYSQL_DB)
    cur = cnx.cursor()
    whis = cnx.cursor()

    id = ''
    cur.execute("SELECT * FROM asterisk WHERE whisper1 = 0 AND whisper = 0 LIMIT 1")
    row = cur.fetchone()
    if row:
        id = row[0]
        whis.execute("UPDATE asterisk SET whisper = 1 WHERE id = %s", (id,))
        cnx.commit()
        whis.close()
        start = row[1]
        finish = row[2]
        voip_file = row[4]
        cur.close()
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        time.sleep(3)
        with pysftp.Connection(host=SFTP_HOST,
                               username=SFTP_USER,
                               password=SFTP_PASSWORD,
                               cnopts=cnopts) as sftp:
            name = id
            try:
                # sftp.get('/mnt/samba/mp3/' + voip_file + name + '.mp3', 'C:/workspace/93/'+ name + '.mp3')
                sftp.get('/mnt/samba/mp3/' + voip_file + name + '.mp3', '/opt/python/' + name + '.mp3')
            except:
                to_whisper = False
                if os.path.isfile(name + '.mp3'):
                    os.remove(name + '.mp3')
                logging.error(f'--{id} не получилось скачать')
                continue
            sftp.close()
            if not row[8]:
                os.system('ffmpeg -i ' + id + '.mp3 -vn -ar 44100 -ac 2 -ab 192 -f wav ' + id + '.wav')
                with wave.open(id + ".wav") as audio_file:
                    channels = audio_file.getnchannels()
                    sampwidth = audio_file.getsampwidth()
                    framerate = audio_file.getframerate()
                    nframes = audio_file.getnframes()
                    samples = audio_file.readframes(nframes)
                    values = list(struct.unpack("<" + str(nframes * channels) + "h", samples))
                    values_copy = values[:]

                    if to_stereo:
                        whisis = cnx.cursor()
                        whisis.execute("UPDATE asterisk SET stereo = 1 WHERE id = %s", (id,))
                        cnx.commit()
                        whisis.close()

                        for index in range(len(values_copy)):
                            if index % 2 == 0:
                                values_copy[index] = 0
                        create_file_one_channel('1_channel.wav', framerate, sampwidth, channels, values_copy)
                        print('1chan')

                        values_copy = values[:]
                        for index in range(len(values_copy)):
                            if index % 2 != 0:
                                values_copy[index] = 0
                        create_file_one_channel('2_channel.wav', framerate, sampwidth, channels, values_copy)
                        print('2chan')
                audio_file.close()

            nn = id + '.wav'
            if to_whisper:
                try:
                    a = datetime.now()
                    audio = whisperx.load_audio(nn)
                    result = model.transcribe(audio, batch_size=batch_size, language='ru')
                    b = str(datetime.now() - a)
                    os.remove(nn)
                    whisi = cnx.cursor()
                    whisi.execute("UPDATE asterisk SET whisper1 = 1 WHERE id = %s", (id,))
                    cnx.commit()
                    whisi.close()
                    aa = cnx.cursor()
                    aa.execute('DELETE FROM whisper WHERE id = %s', (id,))
                    cnx.commit()
                    aa.close()
                    sql = "INSERT INTO whisper (id, text_, titime, employee, friend) VALUES (%s, %s, %s, %s, %s)"
                    val = (id, to_string(result['segments']), b.split('.')[0], "1", "2")
                    wis = cnx.cursor()
                    wis.execute(sql, val)
                    cnx.commit()
                    wis.close()
                except:
                    logging.error(f'------{id} ломаный файл')
                    whisi = cnx.cursor()
                    whisi.execute("UPDATE asterisk SET whisper1 = 1 WHERE id = %s", (id,))
                    cnx.commit()
                    whisi.close()
                    os.remove(id + '.mp3')
                    os.remove(nn)
                if to_stereo:
                    channels_whis('1_channel.wav', id, row[11])
                    if os.path.isfile('1_channel.wav'):
                        os.remove('1_channel.wav')
                    channels_whis('2_channel.wav', id, row[11])
                    if os.path.isfile('2_channel.wav'):
                        os.remove('2_channel.wav')
    if os.path.isfile(id + '.mp3'):
        os.remove(id + '.mp3')
    cnx.close()