from google.cloud import pubsub_v1

PROJECT_ID = "analytic-demo-454105"
SUBSCRIPTION_NAME = "aggregated-ad-clicked-event-sub"


def callback(message):
    """Callback function to handle incoming Pub/Sub messages."""
    print(f"Received message: {message.data.decode('utf-8')}")
    
    message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    main()
