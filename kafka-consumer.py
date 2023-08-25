# Consumer code for Apache Kafka.
from confluent_kafka import Consumer, KafkaError
from datadog import initialize, statsd
import time

options = {
    'statsd_host':'127.0.0.1',
    'statsd_port':8125
}

initialize(**options)
# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker's address
    'group.id': 'sensor_data_group',  # Consumer group ID
    'auto.offset.reset': 'earliest',       
    'enable.auto.commit': False      
}

# Create the Kafka consumer instance
consumer = Consumer(consumer_config)
consumer.subscribe(['sensor_data'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages with a timeout of 1 second
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition.')
            else:
                print(f'Error while consuming: {msg.error()}')
        else:
            # Process the received byte stream data here
            byte_stream = msg.value()
            headers = msg.headers()
            for header in headers:
                if header[0] == 'kafka_start_time':
                    kafka_start_time = int(header[1])
                    
            latency =  abs(int(time.time() * 1000) - kafka_start_time)
            print(f'Received Message. /tLatency: ' + str(latency))
            statsd.distribution('kafka_latency', latency)
            
            consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
