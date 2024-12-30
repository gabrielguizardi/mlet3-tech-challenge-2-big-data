import os
import pandas as pd
from config.settings import DOWNLOAD_DIR

def csv_to_parquet(file_name):
    """Converte um arquivo CSV para Parquet."""
    csv_path = os.path.join(DOWNLOAD_DIR, file_name)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo CSV {file_name} não encontrado.")
    
    # Lê o CSV
    df = pd.read_csv(
        csv_path,
        encoding="ISO-8859-1",
        sep=";",
        skiprows=1,
        skipfooter=2,
        engine="python",
        index_col=False
    )

    # Define o caminho de saída
    parquet_path = csv_path.replace(".csv", ".parquet")

    # Converte para Parquet
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"Arquivo convertido para Parquet: {parquet_path}")
