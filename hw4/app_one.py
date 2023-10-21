# george trammell
from flask import Flask, request
from google.cloud import storage, logging, pubsub_v1

app = Flask(__name__)
banned_countries = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
@app.route('/<path:filename>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
def serve_file(filename):

    # google cloud project info
    bucket_name = 'ds561-hw2-html-linked-bucket'
    logging_client = logging.Client(project='robust-builder-398218')
    logger = logging_client.logger('ds561')
    pub = pubsub_v1.PublisherClient()
    path = pub.topic_path('robust-builder-398218', 'ds561')

    if request.method == 'GET':
        country = request.headers.get("X-country", "")

        if country in banned_countries:
            try:
                data = str({'400 Forbidden Country': country})
                future = pub.publish(path, data.encode("utf-8"))
                mID = future.result()
                logger.log_text(f"Message published with ID {mID}")
            except Exception as e:
                logger.log_text(f"PubSub failed with error {str(e)}")
            logger.log_text(f"Error 400 Forbidden Country: {str(country)}")
            return "Permission Denied", 400
    
        else:
            try:
                storage_client = storage.Client()
                filename = filename.replace(bucket_name+'/', '')
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(filename)
                content = blob.download_as_text()
                logger.log_text(f"200: {filename}")
                return content, 200
            except Exception as e:
                logger.log_text(f"Error 404 {filename} Not Found: {str(e)}")
                return 'File not found', 404
            
    else:
        logger.log_text(f"Error 501 {request.method} Not Implemented")
        return 'Not Implemented', 501

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)