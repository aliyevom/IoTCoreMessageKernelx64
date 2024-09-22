import json
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from utils.logger import setup_logger

logger = setup_logger("StateManagement")

class DeviceShadowManager:
    def __init__(self, client_id, host, root_ca, private_key, certificate):
        self.shadow_client = AWSIoTMQTTShadowClient(client_id)
        self.shadow_client.configureEndpoint(host, 8883)
        self.shadow_client.configureCredentials(root_ca, private_key, certificate)
        self.shadow_client.connect()

    def update_device_state(self, device_name, desired_state):
       
        shadow_handler = self.shadow_client.createShadowHandlerWithName(device_name, True)
        payload = {
            "state": {
                "desired": desired_state
            }
        }
        shadow_handler.shadowUpdate(json.dumps(payload), self._on_shadow_update, 5)

    def _on_shadow_update(self, payload, responseStatus, token):
      
        if responseStatus == "accepted":
            logger.info(f"Shadow update accepted: {payload}")
        else:
            logger.error(f"Shadow update rejected: {responseStatus}")

    def get_device_state(self, device_name):
    
        shadow_handler = self.shadow_client.createShadowHandlerWithName(device_name, True)
        shadow_handler.shadowGet(self._on_get_shadow, 5)

    def _on_get_shadow(self, payload, responseStatus, token):
      
        if responseStatus == "accepted":
            logger.info(f"Shadow get successful: {payload}")
            return json.loads(payload)["state"]["reported"]
        else:
            logger.error(f"Shadow get failed: {responseStatus}")
            return None
