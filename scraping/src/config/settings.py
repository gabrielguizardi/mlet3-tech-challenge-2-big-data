import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M")
DOWNLOAD_DIR_NAME = 'tmp'
DOWNLOAD_DIR = os.path.join(BASE_DIR, DOWNLOAD_DIR_NAME, TIMESTAMP)

B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")
AWS_RAW_BUCKET = os.getenv("AWS_RAW_BUCKET")

DRIVER_PATH = os.getenv("DRIVER_PATH")
