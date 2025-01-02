import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from config.settings import DOWNLOAD_DIR, B3_URL
from utils.file_utils import wait_for_file

def setup_driver():
    """Configura o WebDriver para AWS Lambda."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    download_dir = os.path.abspath(DOWNLOAD_DIR)

    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver_bin = "./driver/chromedriver"
    service = Service(driver_bin)
    return webdriver.Chrome(service=service, options=chrome_options)

def download_csv():
    """Automatiza o download do arquivo CSV."""
    driver = setup_driver()
    try:
        driver.get(B3_URL)
        time.sleep(2)
        download_link = driver.find_element(By.CSS_SELECTOR, 
            "#divContainerIframeB3 > div > div.col-lg-9.col-12.order-2.order-lg-1 > form > div:nth-child(4) > div > div.row.mt-3 > div > div > div.list-avatar-row > div.content > p > a")
        download_link.click()

        downloaded_file = wait_for_file(DOWNLOAD_DIR, timeout=30)

        return downloaded_file
    finally:
        driver.quit()
