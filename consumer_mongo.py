from kafka import KafkaConsumer
from json import loads
import json
from pymongo import MongoClient
from logging import log

# db : local
# collection: enterprise

client = MongoClient('localhost:27017')
db=client.local

# msg =  {"department_id": 2, "department_name": "Fitness"}
# db.enterprise.insert(msg)
# db.enterprise.insert({"department_id": 2, "department_name": "Fitness"})

consumer = KafkaConsumer('mongopoc2',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         consumer_timeout_ms=1000,
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
                         # value_deserializer=forgiving_json_deserializer())
                         # value_deserializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    for message in consumer:
        msg = message.value
        print(msg)
        db.enterprise.insert(msg)
        consumer.commit()