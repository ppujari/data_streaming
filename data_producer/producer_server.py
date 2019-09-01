"""Data Producer base-class providing common utilites and functionality"""
import logging
import json
import csv
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer

logger = logging.getLogger(__name__)
DATA_FILE = './data_producer/resources/police-department-calls-for-service_1000.csv'

def dict_to_binary(json_dict: dict) -> bytes:
    return json.dumps(json_dict).encode('utf-8')


def create_topic():
        """Creates the producer topic if it does not already exist in the Kafka Broker"""
        #
        print("Creating a new topic")
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
        topic_list = []
        topic_list.append(NewTopic(name="udacity-streaming-project", num_partitions=1, replication_factor=1))

        try:
       	    future = admin_client.create_topics(new_topics=topic_list, validate_only=False,timeoutMs=5)
        except Exception as e:
           logger.error("failed to create topic_list")

def read_file():
    with open(DATA_FILE, 'r') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    return data 

def send_data():
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        data = read_file()
        for i in data:
            message = dict_to_binary(i)
            producer.send('udacity-streaming-project', value=message)
            time.sleep(5)


if __name__ == "__main__":
       create_topic()
       send_data() 
