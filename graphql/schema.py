import graphene
import json
from device_sdk.core_data_extraction import DeviceCoreSDK
from utils.logger import setup_logger

logger = setup_logger("GraphQLAPI")

class Device(graphene.ObjectType):
    device_id = graphene.String()
    power = graphene.String()
    temperature = graphene.Int()
    location = graphene.String()

class Query(graphene.ObjectType):
    device = graphene.Field(Device, device_id=graphene.String())

    def resolve_device(self, info, device_id):
        """
        Resolve the device state using Redis cache.
        """
        cache_key = f"device_{device_id}"
        sdk = DeviceCoreSDK(base_url='http://device-data-api.io/v2/', redis_cache=redis_cache)

        # Fetch from Redis cache or device SDK
        cached_data = redis_cache.get_cache(cache_key)
        if cached_data:
            return Device(**json.loads(cached_data))

        # Fetch data from the SDK and cache it
        data = sdk.fetch_data()
        if data and data['device_id'] == device_id:
            redis_cache.set_cache(cache_key, json.dumps(data))  
            return Device(**data)
        return None

# Create GraphQL schema
schema = graphene.Schema(query=Query)
