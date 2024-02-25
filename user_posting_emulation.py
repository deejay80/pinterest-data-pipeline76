import requests
from time import sleep
import random
import json
from multiprocessing import Process
import sqlalchemy
from sqlalchemy import text, create_engine
from datetime import datetime
random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_size=5, max_overflow=10)
        return engine

new_connector = AWSDBConnector()
invoke_url = "https://f1fxsgva0f.execute-api.us-east-1.amazonaws.com/staging/topics"

def serialize_datetime(obj):
    """Serialize datetime object to string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def send_to_kafka(data, topic):
    records = []
    for row in data:
        # Convert datetime objects to strings
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = serialize_datetime(value)
        records.append({"value": row})
    
    payload = json.dumps({"records": records})
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(invoke_url + f"/{topic}", headers=headers, data=payload)
    
    if response.status_code == 200:
        print(f"Data sent to Kafka topic {topic} successfully.")
    else:
        print(f"Failed to send data to Kafka topic {topic}. Status code: {response.status_code}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Query pin data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pin_result = [dict(row._mapping) for row in pin_selected_row]

            # Query geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            geo_result = [dict(row._mapping) for row in geo_selected_row]

            # Query user data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            user_result = [dict(row._mapping) for row in user_selected_row]

        # Send data to Kafka topics
        send_to_kafka(pin_result, "12d5bb99b7ad.pin")
        send_to_kafka(geo_result, "12d5bb99b7ad.geo")
        send_to_kafka(user_result, "12d5bb99b7ad.user")

if __name__ == "__main__":
    run_infinite_post_data_loop()
