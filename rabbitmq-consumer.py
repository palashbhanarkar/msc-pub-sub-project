# Consumer code for RabbitMQ.

from datadog import initialize, statsd
import pika
import time

options = {
    'statsd_host':'127.0.0.1',
    'statsd_port':8125
}

initialize(**options)

def callback(ch, method, properties, body):
    custom_headers = properties.headers
    if 'rabbit_start_time' in custom_headers:
        rabbit_start_time = custom_headers['rabbit_start_time']
        latency = abs(int(time.time() * 1000) - rabbit_start_time)
    # The callback function is called when a message is received
    print("Message Received. /tLatency: " + str(latency))
    statsd.distribution('rabbitmq_latency', latency)
    

# RabbitMQ server connection parameters
RABBITMQ_HOST = '74.235.18.217'    
EXCHANGE_NAME = 'sensor_data.topic'

# Create a connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

result = channel.queue_declare(queue='sensor_data', durable=True)
QUEUE_NAME = result.method.queue

channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key="sensor_data.key")

# Consumer with the callback function
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
