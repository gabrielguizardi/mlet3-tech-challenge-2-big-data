from selenium_scraper.scraper import download_csv
from data_processing.transform import csv_to_parquet
from utils.file_utils import ensure_dir_exists
from utils.s3_utils import upload_parquet_to_s3
from config.settings import DOWNLOAD_DIR, AWS_RAW_BUCKET

ensure_dir_exists(DOWNLOAD_DIR)
csv_file_path = download_csv()
parquet_file_path = csv_to_parquet(csv_file_path)
upload_parquet_to_s3(parquet_file_path, AWS_RAW_BUCKET)

print(f"Arquivo {csv_file_path} processado com sucesso!")
