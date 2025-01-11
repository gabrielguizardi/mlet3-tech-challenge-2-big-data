import os
from config.aws import client
from datetime import datetime

def upload_parquet_to_s3(file_path, bucket_name):
    """
    Faz upload de um arquivo Parquet para o Amazon S3.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"O arquivo '{file_path}' não foi encontrado.")
    
    today_date= datetime.now().strftime('%Y-%m-%d')
    s3_client = client('s3')
    
    s3_client.upload_file(file_path, bucket_name, 'scraped-data/' + f"{today_date}.parquet")
    print(f"Arquivo '{file_path}' enviado com sucesso para '{bucket_name}/scraped-data/{today_date}.parquet'")
