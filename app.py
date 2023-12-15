import os
import time
from kafka.errors import KafkaError
from kafka import KafkaConsumer

#environment variable production
# BOOTSTRAP_SERVER = os.getenv('BOOTSTRAP_SERVER')
# USERNAME = os.getenv('USERNAME')
# PASSWORD = os.getenv('PASSWORD')
# TOPIC = os.getenv('TOPIC')
# SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
# SASL_MECHANISM = os.getenv('SASL_MECHANISM')
# GROUP_ID = os.getenv('GROUP_ID')
# SSL_CHECK_HOSTNAME = os.getenv('SSL_CHECK_HOSTNAME')

#environment variable dev
BOOTSTRAP_SERVER='kafka.apps.dev.customs.go.id:443'
USERNAME='kafka'
PASSWORD='kafka-secret'
TOPIC='test'
SECURITY_PROTOCOL='SASL_SSL'
SASL_MECHANISM='PLAIN'
GROUP_ID = 'test'
SSL_CHECK_HOSTNAME=False

print("BOOTSTRAP_SERVER: ", BOOTSTRAP_SERVER)
print("USERNAME: ", USERNAME)
print("PASSWORD: ", PASSWORD)
print("TOPIC: ", TOPIC)
print("SECURITY_PROTOCOL: ", SECURITY_PROTOCOL)
print("SASL_MECHANISM: ", SASL_MECHANISM)
print("SASL_MECHANISM: ", SASL_MECHANISM)
print("SSL_CHECK_HOSTNAME: ", SSL_CHECK_HOSTNAME)

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    group_id=GROUP_ID,
    client_id= 'python-pod'+'-'+TOPIC+GROUP_ID,
    SSL_CHECK_HOSTNAME=SSL_CHECK_HOSTNAME,
    # auto_offset_reset='earliest',
)


# Subscribe to a topic
consumer.subscribe(topics=[TOPIC])

# Continuously poll for new messages
while True:
    try:
        msg = consumer.poll(timeout_ms=1000)
    except KafkaError as error:
        print(f"Error: {error}")
        break
    except Exception as ex:
        print(f"Exception: {ex}")
        break
    else:
        print(msg)
        for topic_partition, messages in msg.items():
            for message in messages:
                # Do something with the message
                print(f"Received message: {message.value}")
    time.sleep(5)
# Close the consumer
consumer.close()

