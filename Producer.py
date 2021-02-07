from kafka import KafkaProducer
import yaml
import json

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# with open('/home/victor/inputfile.txt') as f:
#     lines = f.readlines()
#
# for line in lines:
#     producer.send('test2', line.encode('utf-8'))

# producer.send('test','This is a message', 'This is a key', 'Headers', 1, None)
# producer.send('test', 'This is a message from python producer')

# msg = yaml.safe_load('{"id":"1", "name":"oguz"}')
# producer.send('test2', json.dumps(msg).encode('utf-8'))
# producer.send('test2', json.dumps(msg).encode('utf-8'))


data = []
for i in range(10):
    # data.append({'empno': i, 'ename':"num{}".format(i), 'sal':"{}".format(i*100)})
    data.append('{"id":"1", "name":"oguz"}')
# print(data)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
#  push message to kafka topic mongo_poc

try:
    for val in data:
        # print(val)
        # valy = yaml.safe_load('{}'.format(val))
        # producer.send('test2', json.dumps(valy).encode('utf-8'))
        # msg = yaml.safe_load('{"id":"1", "name":"oguz"}')
        producer.send('test2', val)
except Exception as e:
    print(e)