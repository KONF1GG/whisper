from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getnenv('HOST')
PORT =os.getnenv('PORT')
USER = os.getnenv('USER')
PASSWORD = os.getnenv('PASSWORD')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_DB = os.getenv('MYSQL_DB')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')