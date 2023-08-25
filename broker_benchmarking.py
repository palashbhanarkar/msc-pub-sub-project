# Generic benchmarking tool code for RabbitMQ, Apache Pulsar, and Apache Kafka, included with sender code.

import os
import sys
import time
import pika
import pulsar
from kafka import KafkaProducer
from pulsar import Client, Message
from tqdm import tqdm
import concurrent.futures
import threading

chunk_size=1024 # Message size
directory_path = "./Videos/"
max_threads = 2 # Number of threads per sender
sleep_timer = 0.025

RABBITMQ_HOST = '74.235.18.217'  # RabbitMQ server host
RABBITMQ_PORT = 5672  # Default RabbitMQ port
RABBITMQ_USERNAME = 'palash'  # RabbitMQ username
RABBITMQ_PASSWORD = 'Test@123'  # RabbitMQ password
EXCHANGE_NAME = 'sensor_data.topic'  # Queue name

def send_data_to_rabbitmq():
    print("Start Time: " + str(time.time()))
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for i in range(max_threads):
            futures.append(executor.submit(rabbitmq_thread))

        concurrent.futures.wait(futures)
    print("End Time: " + str(time.time()))

def rabbitmq_thread():
    credentials = pika.PlainCredentials('palash', 'Test@123')
    parameters = pika.ConnectionParameters('74.235.18.217', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare a queue to send the file chunks
    channel.exchange_declare(EXCHANGE_NAME, exchange_type="topic", durable=True)

    for root, dirs, files in os.walk(directory_path):
        while True:
            for file in files:
                i=0
                file_path = os.path.join(root, file)
                with open(file_path, 'rb') as file:
                    j=0
                    while True:
                        chunk = file.read(chunk_size)
                        if not chunk: 
                            break  # End of file
                        j+=1
                        custom_headers = {
                                'rabbit_start_time': int(time.time() * 1000),   # Add more headers as needed
                            }
                        properties = pika.BasicProperties(headers=custom_headers)
                        channel.basic_publish(exchange=EXCHANGE_NAME,
                                                routing_key="sensor_data.key",
                                                body=chunk, properties=properties)
                        time.sleep(sleep_timer)
                i+=1

        # Close the connection
    connection.close()

def send_data_to_kafka():
    print("Start Time: " + str(time.time()))
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for i in range(max_threads):
            futures.append(executor.submit(kafka_thread))

        concurrent.futures.wait(futures)
    print("End Time: " + str(time.time()))

def kafka_thread():
    bootstrap_servers='74.235.18.217'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: v)
    kafka_topic_name = "sensor_data"

    for root, dirs, files in os.walk(directory_path):

        while True:
            for file in files:
                i=0
                file_path = os.path.join(root, file)
                with open(file_path, 'rb') as file:
                    j=0
                    while True:
                        chunk = file.read(chunk_size)
                        if not chunk:
                            break  # End of file
                        j+=1
                        headers = [
                            ('kafka_start_time', str(int(time.time() * 1000)).encode('utf-8')), # Add more headers as needed in the same format (key, value) tuple
                        ]

                        producer.send(topic=kafka_topic_name, headers=headers, value=chunk)
                        time.sleep(sleep_timer)
                i+=1

    # Close the connection
    producer.flush()
    producer.close()

def send_data_to_pulsar():
    print("Start Time: " + str(time.time()))
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for i in range(max_threads):
            futures.append(executor.submit(pulsar_thread))

        concurrent.futures.wait(futures)
    print("End Time: " + str(time.time()))

def pulsar_thread():
    pulsar_broker_url = 'pulsar://20.172.140.70:6650'
    pulsar_topic_name = 'non-persistent://sensor_data_tenant/sensor_data_namespace/sensor_data_topic'

    # Create a pulsar client instance
    client = pulsar.Client(pulsar_broker_url)
    print("Created Pulsar Client")

    try:
        # Create a producer on the specified topic
        producer = client.create_producer(pulsar_topic_name)
        print("Created Pulsar Producer")

    # Send the bytestream data to the Pulsar queue
        for root, dirs, files in os.walk(directory_path):
            while True:
                for file in files:
                    i=0
                    file_path = os.path.join(root, file)
                    with open(file_path, 'rb') as file:
                        j=0
                        while True:
                            chunk = file.read(chunk_size)
                            if not chunk:
                                break  # End of file
                            j+=1
                            message_headers = {
                                'pulsar_start_time': str(int(time.time() * 1000)), # Add more headers as needed
                            }
                            producer.send(chunk, message_headers)
                            time.sleep(sleep_timer)
                    i+=1

        # Close the producer
        producer.close()
    except Exception as e:
        print(f"Error while sending data to Pulsar: {e}")
    finally:
        # Close the pulsar client
        client.close()

def main():
    # Access the command-line arguments
    arguments = sys.argv[1:]

    # Check if there are any arguments
    if len(arguments) == 0:
        print("No arguments provided.")
    else:
        print("Arguments provided:")
        if(arguments[0] == "RABBITMQ"):
            send_data_to_rabbitmq()
        elif(arguments[0] == "KAFKA"):
            send_data_to_kafka()
        elif(arguments[0] == "PULSAR"):
            send_data_to_pulsar()
        else:
            print("Wrong Input")

if __name__ == "__main__":
    main()
