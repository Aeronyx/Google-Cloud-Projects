# george trammell
from flask import Flask, request
from google.cloud import storage, logging, pubsub_v1
from google.cloud.sql.connector import Connector
import os
from datetime import datetime
import sqlalchemy
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv(".env")

class DatabaseConnector:
    def __init__(self):
        db_connection_string = os.getenv('DB_CONNECTION_STRING')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_database = os.getenv('DB_DATABASE')

        self.connector = Connector()

        def getconn():
            return self.connector.connect(
                db_connection_string,
                "pymysql",
                user=db_user,
                password=db_password,
                db=db_database
            )

        self.pool = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=getconn
        )

class Logger:
    def __init__(self):
        project_id = 'robust-builder-398218'
        logger_name = 'ds561-logging'
        
        self.logging_client = logging.Client(project=project_id)
        self.logger = self.logging_client.logger(logger_name)

    def log(self, message):
        self.logger.log_text(message)


class PubSub:
    def __init__(self):
        project_id = 'robust-builder-398218'
        topic_name = 'hw10-ds561-topic1'
        
        self.pub_client = pubsub_v1.PublisherClient()
        self.topic_path = self.pub_client.topic_path(project_id, topic_name)
        
        self.logger = Logger()

    def publish(self, message):
        try:
            data = message.encode('utf-8')
            future = self.pub_client.publish(self.topic_path, data)
            message_id = future.result()
            self.logger.log(f"Message published with ID: {message_id}")
        except Exception as e:
            self.logger.log(f"PubSub Notification Failed: {str(e)}")

class DatabaseManager:
    def __init__(self):
        self.logger = Logger()
        self.error_codes = {
            9001: "MISSING HEADERS",
            400: "FORBIDDEN",
            404: "NOT FOUND",
            501: "NOT IMPLEMENTED",
        }
        self.dbConnector = DatabaseConnector()

    def insert_client(self, country, client_ip, gender, age, income, banned):
        with self.dbConnector.pool.connect() as connection:
            try:
                result = connection.execute(
                    sqlalchemy.text("SELECT client_id FROM clients WHERE client_ip = :client_ip"),
                    {"client_ip": client_ip}
                ).first()
                client_id = result[0] if result else None

                if client_id is None:
                    connection.execute(
                        sqlalchemy.text(
                            "INSERT INTO clients (client_ip, country, gender, age, income, banned) "
                            "VALUES (:client_ip, :country, :gender, :age, :income, :banned)"
                        ),
                        {
                            "client_ip": client_ip,
                            "country": country,
                            "gender": gender,
                            "age": age,
                            "income": income,
                            "banned": banned
                        }
                    )
                    client_id = connection.execute(sqlalchemy.text("SELECT LAST_INSERT_ID()")).scalar()
                connection.commit()
                return client_id

            except Exception as e:
                self.logger.log(f"insert_client sql failed: {str(e)}")
                connection.rollback()

    def insert_request(self, time_of_day, file_id, client_id, error_code=None):
        with self.dbConnector.pool.connect() as connection:
            try:
                if error_code:
                    # Insert into bad_requests
                    connection.execute(
                        sqlalchemy.text(
                            "INSERT INTO bad_requests (client_id, file_id, request_time, error_code) "
                            "VALUES (:client_id, :file_id, :request_time, :error_code)"
                        ),
                        {
                            "client_id": client_id,
                            "file_id": file_id,
                            "request_time": time_of_day,
                            "error_code": error_code
                        }
                    )
                else:
                    # Insert into requests
                    connection.execute(
                        sqlalchemy.text(
                            "INSERT INTO requests (client_id, file_id, request_time) "
                            "VALUES (:client_id, :file_id, :request_time)"
                        ),
                        {
                            "client_id": client_id,
                            "file_id": file_id,
                            "request_time": time_of_day
                        }
                    )
                connection.commit()
            except Exception as e:
                self.logger.log(f"insert_request sql failed: {str(e)}")
                connection.rollback()

    def handle_database(self, country, client_ip, gender, age, income, banned, time_of_day, requested_file, error_code):
        try:
            file_id = requested_file
            client_id = self.insert_client(country, client_ip, gender, age, income, banned)
            
            self.insert_request(time_of_day, file_id, client_id, error_code)
            
            self.logger.log('Handling database completed successfully')
        except Exception as e:
            self.logger.log(f'Error in handle_database: {e}')

# Main Application Service
class AppService:
    BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar",
                        "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

    def __init__(self):
        self.logger = Logger()
        self.pubsub = PubSub()
        self.db_manager = DatabaseManager()

    def handle_request(self, filename, request_method, headers):
        country = headers.get("X-country")
        client_ip = headers.get("X-client-IP")
        gender = headers.get("X-gender")
        age = headers.get("X-age")
        income = headers.get("X-income")

        if not (country and client_ip and gender and age and income):
            error_code = 9001  # Custom Error Code
            self.logger.log("Error Code 9001: No Header")
            return 'No Headers', error_code

        banned = country in AppService.BANNED_COUNTRIES

        time_of_day = datetime.now().strftime('%H:%M:%S')

        requested_file = filename.replace('hw10-html-bucket/', '')

        error_code = None

        if request_method == 'GET':
            if banned:
                data = str({'400 Forbidden from country': country})
                self.pubsub.publish(data)
                self.logger.log(f"Error Code 400: Forbidden: {str(country)}")
                error_code = 400
                self.db_manager.handle_database(country, client_ip, gender, age, income, banned, time_of_day, requested_file, error_code)
                return "Permission Denied", error_code

            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket('hw10-html-bucket')
                blob = bucket.blob(requested_file)
                file_content = blob.download_as_text()
                self.logger.log(f"200: {requested_file}")
                self.db_manager.handle_database(country, client_ip, gender, age, income, banned, time_of_day, requested_file, error_code)
                return file_content, 200
            except Exception as e:
                self.logger.log(f"Error Code 404: {requested_file}: {str(e)}")
                error_code = 404
                self.db_manager.handle_database(country, client_ip, gender, age, income, banned, time_of_day, requested_file, error_code)
                return 'File not found', error_code

        else:
            error_code = 501
            self.db_manager.handle_database(country, client_ip, gender, age, income, banned, time_of_day, requested_file, error_code)
            self.logger.log(f"Error Code 501: {request_method}")
            return 'Not implemented', error_code

# Flask route handling
service = AppService()

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
@app.route('/<path:filename>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
def app_one(filename):
    return service.handle_request(filename, request.method, request.headers)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)