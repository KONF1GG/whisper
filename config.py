from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getenv('HOST')
PORT =os.getenv('PORT')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_DB = os.getenv('MYSQL_DB')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')