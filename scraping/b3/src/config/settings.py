import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Diretório para downloads
DOWNLOAD_DIR_NAME = 'tmp'
DOWNLOAD_DIR = os.path.join(BASE_DIR, DOWNLOAD_DIR_NAME)

# URL alvo
B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
