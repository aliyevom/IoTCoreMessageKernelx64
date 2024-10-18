import requests
import redis
import json
from time import sleep
from utils.logger import setup_logger

logger = setup_logger("CoreDataExtraction")

class DeviceCoreSDK:
    def __init__(self, base_url, redis_cache, max_retries=3, retry_delay=5, timeout=10):
        """
        Initialize SDK with a Redis cache connection and retry logic.
        :param redis_cache: Redis cache instance for caching device data.
        """
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.cache = redis_cache

    def fetch_data(self):
        cache_key = 'device_data'
        cached_data = self.cache.get_cache(cache_key)
        
        if cached_data:
            return json.loads(cached_data)  # Return cached data if available
        
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(f'{self.base_url}/getDeviceData', timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                self.cache.set_cache(cache_key, json.dumps(data))  # Cache the data
                logger.info("Successfully fetched data from device SDK.")
                return data
            except requests.exceptions.Timeout:
                retries += 1
                backoff = self._calculate_backoff(retries)
                logger.warning(f"Timeout occurred. Retrying {retries}/{self.max_retries} in {backoff:.2f} seconds...")
                sleep(backoff)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from device SDK: {e}")
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

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0, ttl=300):
        """
        Initialize the Redis cache connection.
        :param ttl: Time-to-live for cached data in seconds.
        """
        self.ttl = ttl
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def set_cache(self, key, value):
        """
        Store data in Redis with a specified TTL.
        :param key: Cache key.
        :param value: Data to cache (must be serializable).
        """
        try:
            self.redis_client.setex(key, self.ttl, value)
            logger.info(f"Data cached for key: {key}")
        except redis.RedisError as e:
            logger.error(f"Error caching data: {e}")

    def get_cache(self, key):
        """
        Retrieve data from Redis cache.
        :param key: Cache key.
        :return: Cached data or None if the key is not found.
        """
        try:
            value = self.redis_client.get(key)
            if value:
                logger.info(f"Cache hit for key: {key}")
            else:
                logger.info(f"Cache miss for key: {key}")
            return value
        except redis.RedisError as e:
            logger.error(f"Error retrieving data from cache: {e}")
            return None
