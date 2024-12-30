from selenium_scraper.scraper import download_csv
from data_processing.transform import csv_to_parquet
from utils.file_utils import ensure_dir_exists
from config.settings import DOWNLOAD_DIR

ensure_dir_exists(DOWNLOAD_DIR)
csv_file_name = download_csv()
csv_to_parquet(csv_file_name)

print(f"Arquivo {csv_file_name} processado com sucesso!")
