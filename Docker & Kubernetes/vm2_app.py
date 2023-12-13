# george trammell
from flask import Flask
from google.cloud import pubsub_v1, logging
import json

app = Flask(__name__)

project_id = "robust-builder-398218"
topic_name = "ds561-hw3-banned-requests"
subscription_name = "eventarc-us-east1-ds561-hw3-func-final-582059-sub-826"
banned_countries = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

logging_client = logging.Client()
logger = logging_client.logger("forbidden_country_logger")
subscriber = pubsub_v1.SubscriberClient()

@app.route('/')
def callback(message):
    data = json.loads(message.data.decode('utf-8'))
    country = data.get('country')

    if country in banned_countries:
        logger.log_text(f"Forbidden country detected: {country}")
        print(f"Forbidden country detected: {country}")
    message.ack()

subscription_path = subscriber.subscription_path(project_id, subscription_name)
subscriber.subscribe(subscription_path, callback=callback)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)