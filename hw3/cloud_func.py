# George Trammell
import functions_framework
from google.cloud import storage
from google.cloud import pubsub_v1
import logging

@functions_framework.http
def serve_file(request):

    if request.method != 'GET':
        return ('Not yet implemented.', 501)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_name = 'projects/robust-builder-398218/topics/ds561-hw3-banned-requests-sub'
    response = subscriber.pull(subscription_name, max_messages=10)
    if response != None:
        print(response)

    for received_message in response.received_messages:
        print(received_message.message.data)
        received_message.ack()

    names = request.path.strip('/').split('/')
    if len(names) < 3:
        return ('Not Found', 404)

    bucket_name = names[0]
    username = names[1]
    file_name = names[2]

    headers = ["X-country", "X-client-IP", "X-time"]
    for header in headers:
        logging.info(f"{header}: {request.headers.get(header, 'N/A')}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(f"{username}/{file_name}")
    if not blob.exists():
        logging.error(f"File not found: {username}/{file_name} in bucket {bucket_name}")
        return ('Not Found', 404)

    file_content = blob.download_as_text()
    return (file_content, 200)