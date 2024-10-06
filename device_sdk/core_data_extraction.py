import requests
import random
from time import sleep
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger("CoreDataExtraction")

class DeviceCoreSDK:
    def __init__(self, base_url, max_retries=3, retry_delay=5, timeout=10):
        """
        Initialize the SDK with a base URL, retry logic, and timeout.
        
        :param base_url: The base URL of the device API.
        :param max_retries: Maximum number of retry attempts.
        :param retry_delay: Base delay between retries (exponential backoff will be applied).
        :param timeout: Timeout for the API request in seconds.
        """
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

    def fetch_data(self):
        """
        Fetch data from the device API with retry logic and exponential backoff.
        """
        retries = 0

        while retries < self.max_retries:
            try:
                logger.info(f"Attempt {retries + 1} to fetch data from {self.base_url}/getDeviceData")
                
                # Make the GET request to fetch device data
                response = requests.get(f'{self.base_url}/getDeviceData', timeout=self.timeout)

                # Raise an exception for HTTP error responses
                response.raise_for_status()

                # Log success and return the response data
                logger.info("Successfully fetched data from device SDK.")
                return response.json()

            except requests.exceptions.Timeout:
                retries += 1
                backoff = self._calculate_backoff(retries)
                logger.warning(f"Timeout occurred. Retrying {retries}/{self.max_retries} in {backoff:.2f} seconds...")
                sleep(backoff)

            except requests.exceptions.ConnectionError:
                retries += 1
                backoff = self._calculate_backoff(retries)
                logger.warning(f"Connection error. Retrying {retries}/{self.max_retries} in {backoff:.2f} seconds...")
                sleep(backoff)

            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error occurred: {http_err}")
                # HTTP errors are non-retryable, return immediately
                return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error occurred: {e}")
                return None

        logger.error(f"Max retries ({self.max_retries}) reached. Failed to fetch data.")
        return None

    def _calculate_backoff(self, retries):
        """
        Calculates the backoff time using exponential backoff with jitter.
        
        :param retries: The number of retries already attempted.
        :return: The backoff time in seconds.
        """
        return self.retry_delay * (2 ** (retries - 1)) + random.uniform(0, 1)

