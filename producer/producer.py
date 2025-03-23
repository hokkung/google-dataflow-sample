import json
import random
import pytz
import uuid
import time

from datetime import datetime
from google.cloud import pubsub_v1


project_id = "analytic-demo-454105"
topic_id = "events-topic"
actions = ['CLICK', 'IMPRESSION']
ids = ['id1', 'id2', 'id3', 'id4']
item_types = ['AD', 'OG']

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)    

    for i in range(20):
        time.sleep(random.randint(0,1))
        action = actions[random.randint(0,1)]
        event_value = ids[random.randint(0,3)]
        item_type = item_types[random.randint(0,1)]
        message = {
            "timestamp": datetime.now(tz=pytz.UTC).isoformat(),
            "event_type": action,
            "event_value": event_value,
            "user_id": str(uuid.uuid4())
        }
        attributes = {
            "event_type": action,
            "item_type": item_type
        }

        future = publisher.publish(
            topic_path, 
            json.dumps(message).encode("utf-8"),
            **attributes
        )
        print(f"attributes={attributes} message={message}")
        print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    main()
