from google.cloud import pubsub_v1

def recieve(message):
    print(f"Message Recieved: {message.data.decode('utf-8')}")
    message.ack()

sub = pubsub_v1.SubscriberClient()
path = sub.subscription_path('robust-builder-398218', 'ds561-sub')
future = sub.subscribe(path, callback=recieve)

print(f"Listening on {path}")

with sub:
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()


