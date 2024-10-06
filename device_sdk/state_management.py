import time
import json
import random
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger("StateManagement")

# Constants
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # Seconds to wait before retrying with exponential backoff

class DeviceShadowManager:
    def __init__(self, client_id, host, root_ca, private_key, certificate):
        """
        Initializes the MQTT client and connects to the AWS IoT Core endpoint.
        """
        self.shadow_client = AWSIoTMQTTShadowClient(client_id)
        self.shadow_client.configureEndpoint(host, 8883)
        self.shadow_client.configureCredentials(root_ca, private_key, certificate)
        self._connect()

    def _connect(self):
        """
        Connects to the AWS IoT Core MQTT endpoint with retry logic.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info("Connecting to AWS IoT Core MQTT endpoint...")
                self.shadow_client.connect()
                logger.info("Successfully connected to AWS IoT Core.")
                break
            except Exception as e:
                logger.error(f"Connection failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    logger.info(f"Retrying connection in {backoff:.2f} seconds...")
                    time.sleep(backoff)
                else:
                    raise ConnectionError("Failed to connect to AWS IoT Core after multiple attempts.")

    def update_device_state(self, device_name, desired_state):
        """
        Updates the device shadow with the desired state.
        Implements retry logic for shadow updates.
        """
        if not self._validate_desired_state(desired_state):
            logger.error(f"Invalid desired state: {desired_state}")
            return False

        shadow_handler = self.shadow_client.createShadowHandlerWithName(device_name, True)
        payload = {
            "state": {
                "desired": desired_state
            }
        }

        # Attempt shadow update with retries
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Attempting to update shadow for {device_name}: {desired_state}")
                shadow_handler.shadowUpdate(json.dumps(payload), self._on_shadow_update, 5)
                return True
            except Exception as e:
                logger.error(f"Shadow update failed on attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    logger.info(f"Retrying shadow update in {backoff:.2f} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Failed to update device shadow after {MAX_RETRIES} attempts.")
                    return False

    def get_device_state(self, device_name):
        """
        Fetches the current device shadow state. Implements retry logic for shadow gets.
        """
        shadow_handler = self.shadow_client.createShadowHandlerWithName(device_name, True)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Fetching shadow for device {device_name}")
                shadow_handler.shadowGet(self._on_get_shadow, 5)
                return True
            except Exception as e:
                logger.error(f"Shadow get failed on attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    logger.info(f"Retrying shadow get in {backoff:.2f} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Failed to get device shadow after {MAX_RETRIES} attempts.")
                    return False

    def _on_shadow_update(self, payload, responseStatus, token):
        """
        Callback handler for the shadow update response.
        """
        if responseStatus == "accepted":
            logger.info(f"Shadow update accepted: {payload}")
        else:
            logger.error(f"Shadow update rejected: {responseStatus}")

    def _on_get_shadow(self, payload, responseStatus, token):
        """
        Callback handler for the shadow get response.
        """
        if responseStatus == "accepted":
            logger.info(f"Shadow get successful: {payload}")
            reported_state = json.loads(payload).get("state", {}).get("reported", {})
            logger.info(f"Reported state: {reported_state}")
            return reported_state
        else:
            logger.error(f"Shadow get failed: {responseStatus}")
            return None

    def _validate_desired_state(self, state):
        """
        Validates the desired state before updating the device shadow.
        Returns True if the state is valid, False otherwise.
        """
        if not isinstance(state, dict):
            return False
        # Example validation: Ensure "power" and "temperature" fields are present
        if "power" not in state or state["power"] not in ["on", "off"]:
            return False
        if "temperature" in state and not (10 <= state["temperature"] <= 35):
            return False
        return True
