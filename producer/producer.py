import json
import random
from google.cloud import pubsub_v1


project_id = "analytic-demo-454105"
topic_id = "events"
actions = ['click', 'impression']

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)    

    for i in range(10):
        action = actions[random.randint(0,1)]
        message = {
            "timestamp": "2024-03-18T12:54:09.66644+07:00",
            "event_type": action,
            "event_value": "1",
        }
        attributes = {
            "event_type": action,
        }

        future = publisher.publish(
            topic_path, 
            json.dumps(message).encode("utf-8"),
            **attributes
        )
        print(message)
        print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    main()
