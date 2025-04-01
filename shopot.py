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

# Константы
CURRENT_DIR = os.getcwd()
LOG_FILE = os.path.join(CURRENT_DIR, 'whisp.log')
TEMP_DIR = os.path.join(CURRENT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Инициализация виспер модели
device = 'cuda'
batch_size = 4
compute_type = 'int8'
model = whisperx.load_model("large-v3", device, compute_type=compute_type, language='ru')

def to_string(segments):
    return ''.join(str(seg['text']) for seg in segments)

def get_db_connection():
    return mysql.connector.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        database=MYSQL_DB
    )

def process_stereo_channels(id, values, framerate, sampwidth, channels):
    try:
        cnx = get_db_connection()
        with cnx.cursor() as cursor:
            cursor.execute("UPDATE asterisk SET stereo = 1 WHERE id = %s", (id,))
            cnx.commit()

        # Обработка 1 канала
        ch1_values = [0 if i % 2 == 0 else val for i, val in enumerate(values)]
        ch1_path = os.path.join(TEMP_DIR, f'{id}_1_channel.wav')
        create_wav_file(ch1_path, framerate, sampwidth, channels, ch1_values)

        # Обработка 2 канала
        ch2_values = [0 if i % 2 != 0 else val for i, val in enumerate(values)]
        ch2_path = os.path.join(TEMP_DIR, f'{id}_2_channel.wav')
        create_wav_file(ch2_path, framerate, sampwidth, channels, ch2_values)

        return ch1_path, ch2_path
    finally:
        cnx.close()

def create_wav_file(path, framerate, sampwidth, channels, values):
    with wave.open(path, "wb") as out_file:
        out_file.setframerate(framerate)
        out_file.setsampwidth(sampwidth)
        out_file.setnchannels(channels)
        audio_data = struct.pack(f"<{len(values)}h", *values)
        out_file.writeframes(audio_data)

def download_file(sftp, remote_path, local_path):
    try:
        sftp.get(remote_path, local_path)
        return True
    except Exception as e:
        logging.error(f"Download failed: {str(e)}")
        return False

def process_audio(id, voip_file, caller):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    temp_files = []

    try:
        # Загрузка mp3
        mp3_path = os.path.join(TEMP_DIR, f'{id}.mp3')
        remote_mp3_path = f'/mnt/samba/mp3/{voip_file}{id}.mp3'

        with pysftp.Connection(
            host=SFTP_HOST,
            username=SFTP_USER,
            password=SFTP_PASSWORD,
            cnopts=cnopts
        ) as sftp:
            if not download_file(sftp, remote_mp3_path, mp3_path):
                return

        # Конвертируем в WAV
        wav_path = os.path.join(TEMP_DIR, f'{id}.wav')
        os.system(f'ffmpeg -i {mp3_path} -vn -ar 44100 -ac 2 -ab 192 -f wav {wav_path}')
        temp_files.append(wav_path)

        with wave.open(wav_path) as audio_file:
            channels = audio_file.getnchannels()
            sampwidth = audio_file.getsampwidth()
            framerate = audio_file.getframerate()
            nframes = audio_file.getnframes()
            samples = audio_file.readframes(nframes)
            values = list(struct.unpack(f"<{nframes * channels}h", samples))

        ch1_path, ch2_path = process_stereo_channels(id, values, framerate, sampwidth, channels)
        temp_files.extend([ch1_path, ch2_path])

        # Использование модели для транскрипции
        start_time = datetime.now()
        audio = whisperx.load_audio(wav_path)
        result = model.transcribe(audio, batch_size=batch_size, language='ru')
        processing_time = str(datetime.now() - start_time).split('.')[0]

        # Обновляем базу данных
        cnx = get_db_connection()
        try:
            with cnx.cursor() as cursor:
                cursor.execute("UPDATE asterisk SET whisper1 = 1 WHERE id = %s", (id,))
                
                cursor.execute('DELETE FROM whisper WHERE id = %s', (id,))
                insert_query = """
                    INSERT INTO whisper 
                    (id, text_, titime, employee, friend) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    id, 
                    to_string(result['segments']), 
                    processing_time, 
                    "1", 
                    "2"
                ))
                cnx.commit()

            channels_whis(ch1_path, id, caller)
            channels_whis(ch2_path, id, caller)

        finally:
            cnx.close()

    except Exception as e:
        logging.error(f"Processing failed for {id}: {str(e)}")
    finally:
        for f in temp_files + [mp3_path]:
            if os.path.exists(f):
                os.remove(f)

def channels_whis(file_path, id, caller):
    try:
        cnx = get_db_connection()
        channel_number = int(os.path.basename(file_path).split('_')[0])
        audio = whisperx.load_audio(file_path)
        result = model.transcribe(audio, batch_size=batch_size, language='ru')
        
        with cnx.cursor() as cursor:
            if (channel_number == 1 and caller == 'incoming') or (channel_number == 2 and caller == 'outcoming'):
                cursor.execute('UPDATE whisper SET employee = %s WHERE id = %s', 
                              (to_string(result['segments']), id))
            else:
                cursor.execute('UPDATE whisper SET friend = %s WHERE id = %s', 
                              (to_string(result['segments']), id))
            cnx.commit()
    finally:
        cnx.close()

def main_loop():
    while True:
        try:
            cnx = get_db_connection()
            with cnx.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM asterisk 
                    WHERE whisper1 = 0 AND whisper = 0 
                    LIMIT 1
                """)
                row = cursor.fetchone()

                if row:
                    id = row[0]
                    try:
                        with cnx.cursor() as update_cursor:
                            update_cursor.execute(
                                "UPDATE asterisk SET whisper = 1 WHERE id = %s",
                                (id,)
                            )
                            cnx.commit()
                        
                        process_audio(
                            id=id,
                            voip_file=row[4],
                            caller=row[11]
                        )

                    except Exception as e:
                        logging.error(f"Main loop error for {id}: {str(e)}")
                        cnx.rollback()

        except Exception as e:
            logging.error(f"Database connection error: {str(e)}")
        finally:
            if 'cnx' in locals() and cnx.is_connected():
                cnx.close()
            time.sleep(1)

if __name__ == "__main__":
    main_loop()