# Consumer code for Apache Pulsar.

from pulsar import ConsumerType, MessageId, ConsumerType
from datadog import initialize, statsd
import pulsar
import time

options = {
    'statsd_host':'127.0.0.1',
    'statsd_port':8125
}

initialize(**options)

# Replace the below variables with your Pulsar connection details
service_url = 'pulsar://localhost:6650'
topic = 'non-persistent://sensor_data_tenant/sensor_data_namespace/sensor_data_topic'
subscription_name = 'sensor_data_subscription'

client = pulsar.Client(service_url)

try:
    consumer = client.subscribe(topic, subscription_name, consumer_type=ConsumerType.Exclusive)

    while True:
        msg = consumer.receive()
        headers = msg.properties()
        pulsar_start_time = headers.get('pulsar_start_time')
        latency = abs(int(time.time() * 1000) - int(pulsar_start_time))
        try:
            consumer.acknowledge(msg)
        except Exception as e:
            # Handle exception or log error
            consumer.negative_acknowledge(msg)
        print("Received message. \tLatency: " + str(latency))
        statsd.distribution('pulsar_latency', latency)

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    client.close()
