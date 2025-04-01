from datetime import datetime
import mysql.connector
import wave, struct
import pysftp
import os
import whisperx
import logging
from config import MYSQL_HOST, MYSQL_DB, MYSQL_PASSWORD, MYSQL_USER, SFTP_PASSWORD, SFTP_HOST, SFTP_USER

log_file = '/opt/python/logs/whisp.log'
logging.basicConfig(filename=log_file, level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

device = 'cuda'
batch_size = 4
compute_type = 'int8'
model = whisperx.load_model("large-v3", device, compute_type=compute_type)
delmod = 0

def to_string(segment):
  text = ''
  for seg in segment:
    text += str(seg['text']) 
  return text
    

def channels_whis(nn, id):
  sss = mysql.connector.connect(user=MYSQL_USER, 
                                password=MYSQL_PASSWORD,
                                host=MYSQL_HOST,
                                database=MYSQL_DB)
  lin = int(nn.split('_')[0])
  audio = whisperx.load_audio(nn)
  resu = model.transcribe(audio, batch_size=batch_size)
  sqlone = sss.cursor()
  if lin == 1:
    sqlone.execute('update whisper set employee = "' + to_string(resu['segments']) + '" where id = ' + id)
    sss.commit()
  elif lin == 2:
    sqlone.execute('update whisper set friend = "' + to_string(resu['segments']) + '" where id = ' + id)
    sss.commit()
  os.remove(nn)
  sss.close()

def create_file_one_channel(name):
  out_file = wave.open(name, "wb")

  out_file.setframerate(framerate)
  out_file.setsampwidth(sampwidth)
  out_file.setnchannels(CHANNELS)

  audio_data = struct.pack(f"<{N_SAMPLES * CHANNELS}h", *values_copy)
  out_file.writeframes(audio_data)
  
to_whisper = True
to_stereo = False
cnx = mysql.connector.connect(user=MYSQL_USER, 
                              password=MYSQL_PASSWORD,
                              host=MYSQL_HOST,
                              database=MYSQL_DB)
cur = cnx.cursor()

id = ''
#cur.execute("select * from asterisk where voip_file = '2024/05/26/'")
cur.execute("select *  from asterisk where datim > '2024-05-29 15:46:33'")
rows = cur.fetchall()
for row in rows: 
#row = cur.fetchone()
#if row: 
    id = row[0]
    start = row[1]
    finish = row[2]
    voip_file = row[4]
    cur.close()  
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host=SFTP_HOST, 
                           username=SFTP_USER,
                           password=SFTP_PASSWORD,
                           cnopts=cnopts
                           ) as sftp:          
      name = id
      try:
        sftp.get('/mnt/samba/mp3/' + voip_file + name + '.mp3', '/opt/python/'+ name + '.mp3')
      except:
        to_whisper = False
        if os.path.isfile(name + 'mp3'):
          os.remove(name + '.mp3')
        logging.error(f'--' + id + ' не получилось скачать')
        #continue
      sftp.close() 
      os.system('ffmpeg -i ' + id + '.mp3 -vn -ar 44100 -ac 2 -ab 192 -f wav ' + id + '.wav')
      with wave.open(id + ".wav") as audio_file:
          CHANNELS = audio_file.getnchannels()
          SAMPLE_WIDTH = audio_file.getsampwidth()
          CHANNELS = audio_file.getnchannels()
          FRAMERATE = audio_file.getframerate()
          N_SAMPLES = audio_file.getnframes()

          N_FRAMES = audio_file.getnframes()

          nchannels = CHANNELS
          sampwidth = SAMPLE_WIDTH
          framerate = FRAMERATE
          nframes = N_FRAMES

          comptype = "NONE"  # тип компрессии
          compname = "not compressed"  # название компрессии

          N_SAMPLES = nframes
          CHANNELS = nchannels
          to_stereo = True
          if to_stereo:
            samples = audio_file.readframes(N_FRAMES)
            values = list(struct.unpack("<" + str(N_FRAMES * CHANNELS) + "h", samples))
            values_copy = values[:]

            for index, i in enumerate(values_copy):
                if index % 2 == 0:
                  values_copy[index] = 0

            create_file_one_channel('1_channel.wav')
            values_copy = values[:]

            for index, i in enumerate(values_copy):
                if index % 2 != 0:
                  values_copy[index] = 0

            create_file_one_channel('2_channel.wav')
      audio_file.close()  
                  
      nn = id + '.wav'
      if to_whisper:
          try:
            if delmod >= 10:
              del model
              model = whisperx.load_model("large-v3", device, compute_type=compute_type)
              delmod = 0
            a = datetime.now()
            audio = whisperx.load_audio(nn)
            result = model.transcribe(audio, batch_size=batch_size)
            b = str(datetime.now() - a)
            os.remove(nn)   
            sqle = cnx.cursor()
            sqle.execute = ("update whisper set text_ = '" + to_string(result['segments']) + "' where id = " + id)
            cnx.commit()
            sqle.close()
          except:
            logging.error(f'------' + id + 'ломаный файл')
            os.remove(id + '.mp3')
            if os.path.isfile(nn):
              os.remove(nn)
          if to_stereo:
            channels_whis('1_channel.wav', id)
            if os.path.isfile('1_channel.wav'):
              os.remove('1_channel.wav')
            channels_whis('2_channel.wav', id)  
            if os.path.isfile('2_channel.wav'):
              os.remove('2_channel.wav')
    if os.path.isfile(id + '.mp3'):
      os.remove(id + '.mp3')
    cnx.close()