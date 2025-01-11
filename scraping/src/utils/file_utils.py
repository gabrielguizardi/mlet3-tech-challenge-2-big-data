import os
import time

def ensure_dir_exists(directory):
    """Garante que o diretório existe, criando-o se necessário."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_latest_file(download_directory):
    """Retorna o nome do arquivo mais recente em um diretório."""
    files = [os.path.join(download_directory, f) for f in os.listdir(download_directory)]
    files = [f for f in files if os.path.isfile(f)]  # Apenas arquivos

    if not files:
        raise FileNotFoundError("Nenhum arquivo encontrado no diretório de downloads.")

    latest_file = max(files, key=os.path.getmtime)  # Arquivo mais recente
    return latest_file

def wait_for_file(directory, timeout=30, interval=1):
    """Aguarda até que um arquivo apareça no diretório."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            return get_latest_file(directory)
        except FileNotFoundError:
            time.sleep(interval)

    raise TimeoutError("Nenhum arquivo encontrado no diretório dentro do tempo limite.")
