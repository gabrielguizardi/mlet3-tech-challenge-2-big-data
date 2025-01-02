import os
from config.aws import client

def upload_parquet_to_s3(file_path, bucket_name, s3_key):
    """
    Faz upload de um arquivo Parquet para o Amazon S3.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"O arquivo '{file_path}' não foi encontrado.")
    
    s3_client = client('s3')
    
    s3_client.upload_file(file_path, bucket_name, s3_key + '/data.parquet')
    print(f"Arquivo '{file_path}' enviado com sucesso para 's3://{bucket_name}/{s3_key}/data.parquet'")
