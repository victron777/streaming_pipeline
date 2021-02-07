import os
import psycopg2
from dotenv import load_dotenv
from json import dumps
import json
from kafka import KafkaProducer
import time


load_dotenv()


def producers():
    connection = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"])

    cursor = connection.cursor()
    query = "SELECT * FROM departments where department_id > %s"
    cursor.execute(query, ("0"))
    result = cursor.fetchall()

    # print(result)
    data = []

    for row in result:
        # print({"department_id":row[0], "department_name":row[1]})
        data.append({"department_id":row[0], "department_name":row[1]})

    connection.close()

    # for attemp in range(100):
    #     for val in data:
    #         time.sleep(3)
    #         print(val)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for attemp in range(1):
        try:
            for val in data:
                time.sleep(3)
                print(val)
                producer.send("mongopoc2", val)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    producers()