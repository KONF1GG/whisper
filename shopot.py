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
import torch
from GPU_control import gpu_lock

torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

# Константы
CURRENT_DIR = os.getcwd()
LOG_FILE = os.path.join(CURRENT_DIR, 'whisp.log')
TEMP_DIR = os.path.join(CURRENT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)
IDLE_TIMEOUT = 10
MAX_RETRIES = 2  # Максимальное количество повторных попыток

# Настройка логирования
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("pysftp").setLevel(logging.WARNING)

# Глобальные переменные
model = None
last_task_time = None
device = 'cuda'
batch_size = 4
compute_type = 'int8'

def load_model():
    global model
    if model is None:
        logging.info("Загружаю модель Whisper в GPU...")
        model = whisperx.load_model("large-v3", device, compute_type=compute_type, language='ru')
    return model

def unload_model():
    global model
    if model is not None:
        logging.info("Выгружаю модель Whisper из GPU...")
        del model
        torch.cuda.empty_cache()
        model = None

def handle_cuda_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except torch.cuda.OutOfMemoryError as e:
            logging.error(f"CUDA out of memory error in {func.__name__}: {str(e)}")
            unload_model()
            torch.cuda.empty_cache()
            raise
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}")
            raise
    return wrapper

@handle_cuda_errors
def transcribe_audio(model_instance, audio_path):
    audio = whisperx.load_audio(audio_path)
    return model_instance.transcribe(audio, batch_size=batch_size, language='ru')

def get_db_connection():
    return mysql.connector.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        database=MYSQL_DB
    )

def update_database(id, field, value):
    cnx = get_db_connection()
    try:
        with cnx.cursor() as cursor:
            cursor.execute(f"UPDATE asterisk SET {field} = %s WHERE id = %s", (value, id))
            cnx.commit()
    finally:
        cnx.close()

def process_stereo_channels(id, values, framerate, sampwidth, channels):
    try:
        update_database(id, 'stereo', 1)

        ch1_path = os.path.join(TEMP_DIR, f'{id}_1_channel.wav')
        ch2_path = os.path.join(TEMP_DIR, f'{id}_2_channel.wav')

        for path, channel_values in [(ch1_path, values[::2]), (ch2_path, values[1::2])]:
            with wave.open(path, "wb") as out_file:
                out_file.setframerate(framerate)
                out_file.setsampwidth(sampwidth)
                out_file.setnchannels(1)  # Моно канал
                audio_data = struct.pack(f"<{len(channel_values)}h", *channel_values)
                out_file.writeframes(audio_data)

        return ch1_path, ch2_path
    except Exception as e:
        logging.error(f"Error processing stereo channels: {str(e)}")
        raise

def download_file(sftp, remote_path, local_path):
    try:
        sftp.get(remote_path, local_path)
        return True
    except Exception as e:
        logging.error(f"Download failed: {str(e)}")
        return False

def process_audio(id, voip_file, caller):
    global last_task_time
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    temp_files = []

    try:
        # Загрузка и конвертация аудио
        mp3_path = os.path.join(TEMP_DIR, f'{id}.mp3')
        remote_mp3_path = f'/mnt/samba/mp3/{voip_file}{id}.mp3'

        with pysftp.Connection(SFTP_HOST, username=SFTP_USER, password=SFTP_PASSWORD, cnopts=cnopts) as sftp:
            if not download_file(sftp, remote_mp3_path, mp3_path):
                return

        wav_path = os.path.join(TEMP_DIR, f'{id}.wav')
        os.system(f'ffmpeg -i {mp3_path} -vn -ar 44100 -ac 2 -ab 192 -f wav {wav_path} > /dev/null 2>&1')
        temp_files.append(wav_path)

        # Обработка аудио
        with wave.open(wav_path) as audio_file:
            channels = audio_file.getnchannels()
            sampwidth = audio_file.getsampwidth()
            framerate = audio_file.getframerate()
            nframes = audio_file.getnframes()
            samples = audio_file.readframes(nframes)
            values = list(struct.unpack(f"<{nframes * channels}h", samples))

        ch1_path, ch2_path = process_stereo_channels(id, values, framerate, sampwidth, channels)
        temp_files.extend([ch1_path, ch2_path])

        # Обработка с повторными попытками
        for attempt in range(MAX_RETRIES):
            try:
                with gpu_lock(timeout=30):
                    model_instance = load_model()
                    start_time = datetime.now()
                    
                    result = transcribe_audio(model_instance, wav_path)
                    processing_time = str(datetime.now() - start_time).split('.')[0]

                    process_channel(ch1_path, id, caller, model_instance, 1)
                    process_channel(ch2_path, id, caller, model_instance, 2)
                
                update_success(id, processing_time, result)
                last_task_time = time.time()
                break

            except torch.cuda.OutOfMemoryError:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Retrying transcription (attempt {attempt + 1})")
                    unload_model()
                    torch.cuda.empty_cache()
                else:
                    raise

    except Exception as e:
        logging.error(f"Critical error processing {id}: {str(e)}")
        mark_task_failed(id)
    finally:
        cleanup_resources(temp_files + [mp3_path])
        unload_model()

def update_success(id, processing_time, result):
    cnx = get_db_connection()
    try:
        with cnx.cursor() as cursor:
            cursor.execute("UPDATE asterisk SET whisper1 = 1 WHERE id = %s", (id,))
            cursor.execute('DELETE FROM whisper WHERE id = %s', (id,))
            cursor.execute("""
                INSERT INTO whisper 
                (id, text_, titime, employee, friend) 
                VALUES (%s, %s, %s, %s, %s)
            """, (id, ''.join(seg['text'] for seg in result['segments']), processing_time, "1", "2"))
            cnx.commit()
    finally:
        cnx.close()

def mark_task_failed(id):
    try:
        cnx = get_db_connection()
        with cnx.cursor() as cursor:
            cursor.execute("UPDATE asterisk SET whisper = 0 WHERE id = %s", (id,))
            cnx.commit()
    except Exception as e:
        logging.error(f"Failed to mark task as failed: {str(e)}")
    finally:
        if cnx.is_connected():
            cnx.close()

def cleanup_resources(files):
    for f in files:
        try:
            if os.path.exists(f):
                os.remove(f)
        except Exception as e:
            logging.error(f"Error deleting file {f}: {str(e)}")

@handle_cuda_errors
def process_channel(file_path, id, caller, model_instance, channel_number):
    try:
        audio = whisperx.load_audio(file_path)
        result = model_instance.transcribe(audio, batch_size=batch_size, language='ru')
        
        cnx = get_db_connection()
        with cnx.cursor() as cursor:
            if (channel_number == 1 and caller == 'incoming') or (channel_number == 2 and caller == 'outcoming'):
                cursor.execute('UPDATE whisper SET employee = %s WHERE id = %s', 
                             (''.join(seg['text'] for seg in result['segments']), id))
            else:
                cursor.execute('UPDATE whisper SET friend = %s WHERE id = %s', 
                             (''.join(seg['text'] for seg in result['segments']), id))
            cnx.commit()
    finally:
        if cnx.is_connected():
            cnx.close()

def main_loop():
    global last_task_time
    while True:
        try:
            cnx = get_db_connection()
            with cnx.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT * FROM asterisk 
                    WHERE whisper1 = 0 AND whisper = 0 
                    LIMIT 1
                """)
                row = cursor.fetchone()

                if row:
                    process_task(row)
                else:
                    handle_idle_state()
        except Exception as e:
            logging.error(f"Main loop error: {str(e)}")
        finally:
            time.sleep(1)

def process_task(row):
    try:
        cnx = get_db_connection()
        with cnx.cursor() as cursor:
            cursor.execute("UPDATE asterisk SET whisper = 1 WHERE id = %s", (row['id'],))
            cnx.commit()
        
        logging.info(f"Обрабатываю задачу с ID {row['id']}")
        process_audio(
            id=row['id'],
            voip_file=row['voip_file'],
            caller=row['caller']
        )

    except Exception as e:
        logging.error(f"Task processing failed for {row['id']}: {str(e)}")
        mark_task_failed(row['id'])

def handle_idle_state():
    global last_task_time
    try:
        cnx = get_db_connection()
        with cnx.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM asterisk WHERE whisper1 = 0 AND whisper = 0")
            pending_tasks = cursor.fetchone()[0]

            if pending_tasks == 0 and last_task_time and (time.time() - last_task_time) > IDLE_TIMEOUT:
                logging.info("Нет задач, выгружаю модель")
                unload_model()
                last_task_time = None

            if model is not None:
                try_gpu_access()
    finally:
        if cnx.is_connected():
            cnx.close()

def try_gpu_access():
    try:
        with gpu_lock(timeout=0):
            pass
        time.sleep(5)
    except RuntimeError:
        logging.info("GPU занят другим сервисом, выгружаю модель")
        unload_model()
        global last_task_time
        last_task_time = None

if __name__ == "__main__":
    main_loop()