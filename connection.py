"""
Azure Event Hubs Producer Module.
Handles streaming JSON event batches to Azure Event Hubs.
"""

import json
import logging
import os
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from data import generate_uber_ride_confirmation

load_dotenv()

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME", "ubertopic")

logger = logging.getLogger(__name__)


def send_to_event_hub(ride_data=None, batch_size=1):
    """
    Sends a single ride event or batch of ride events to Azure Event Hubs.
    
    Args:
        ride_data (dict): Dictionary representing the generated Uber ride confirmation payload.
        batch_size (int): Number of events per batch.
        
    Returns:
        bool | str: Status message or error boolean.
    """
    if not CONNECTION_STRING or not EVENT_HUBNAME:
        logger.error("CONNECTION_STRING or EVENT_HUBNAME is not configured in .env")
        return "Error: Azure Event Hub credentials not configured"

    try:
        producer = EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        )
        
        ride_json = json.dumps(ride_data)
        event_batch = producer.create_batch()
        event = EventData(ride_json)
        event_batch.add(event)
        
        producer.send_batch(event_batch)
        producer.close()

        return "Successfully sent to Event Hub"
    except Exception as e:
        logger.error(f"Error sending data to Event Hub: {e}")
        return False


if __name__ == "__main__":
    print("=" * 80)
    print("UBER REAL-TIME EVENT STREAM PRODUCER TEST")
    print("=" * 80)
    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2))
    
    print("\n" + "=" * 80)
    print("STREAMING SINGLE EVENT TO AZURE EVENT HUBS")
    result = send_to_event_hub(ride)
    print(f"Result: {result}")