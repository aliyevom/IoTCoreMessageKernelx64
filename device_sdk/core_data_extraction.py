import requests
from utils.logger import setup_logger
from time import sleep

logger = setup_logger("CoreDataExtraction")

class DeviceCoreSDK:
    def __init__(self, base_url, max_retries=3, retry_delay=5):
       
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def fetch_data(self):
       
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(f'{self.base_url}/getDeviceData', timeout=10)
                response.raise_for_status()
                logger.info("Successfully fetched data from device SDK.")
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from device SDK: {e}")
                return None
        logger.error(f"Max retries ({self.max_retries}) reached. Failed to fetch data.")
        return None
