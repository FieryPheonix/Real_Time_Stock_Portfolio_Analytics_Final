import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_data = pd.read_csv('/opt/airflow/data/streaming_dataset/to_be_streamed.csv')

print("Sending data to Kafka...")

for index, row in csv_data.iterrows():
    message = row.to_dict()
    #message['timestamp'] = pd.Timestamp.now().isoformat()
    producer.send('55_0593_Topic', value=message)
    print(f"Sent: {message}")
    time.sleep(0.3)

producer.send('55_0593_Topic', value='EOS')
print("Sent EOS message.")

producer.close()
