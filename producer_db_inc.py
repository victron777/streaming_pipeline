import os
import psycopg2
from dotenv import load_dotenv
from json import dumps
import json
from kafka import KafkaProducer
import time
import datetime


load_dotenv()


def producers():

    query = """
            SELECT *
            FROM departments_inc 
            where abs(DATE_PART('minute', modified_date - current_timestamp)) <= 1
            """


    # cursor.execute(query)
    # for row in cursor.fetchall():
    #     print(row)

    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    #                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    data = []

    for attemp in range(100):

        connection = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"])

        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        query = """
            SELECT *
            FROM departments_inc 
            where abs(DATE_PART('minute', modified_date - current_timestamp)) <= 1
            """

        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        for row in result:
            data.append({"department_id":row[0], "department_name":row[1]})
            print(row)

        try:
            for val in data:
                print(val)
                producer.send("mongopoc2", val)
        except Exception as e:
            print(e)

        data = []
        del cursor
        del result
        del producer
        del query
        print("Finalizing process... \n")
        time.sleep(60)
        print("Initializing process...")

        connection.close()


if __name__ == "__main__":
    producers()