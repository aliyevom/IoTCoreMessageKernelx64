from device_sdk.state_management import DeviceShadowManager
from config.iot_shadow_config import DEVICE_NAME, CLIENT_ID, HOST, ROOT_CA, PRIVATE_KEY, CERTIFICATE
from utils.logger import setup_logger

logger = setup_logger("StateChangeHandler")

def handle_device_state_change():

    shadow_manager = DeviceShadowManager(
        client_id=CLIENT_ID,
        host=HOST,
        root_ca=ROOT_CA,
        private_key=PRIVATE_KEY,
        certificate=CERTIFICATE
    )

    desired_state = {"power": "on", "temperature": 22}
    shadow_manager.update_device_state(DEVICE_NAME, desired_state)

    # Optionally fetch and log the updated state
    updated_state = shadow_manager.get_device_state(DEVICE_NAME)
    if updated_state:
        logger.info(f"Updated state: {updated_state}")
