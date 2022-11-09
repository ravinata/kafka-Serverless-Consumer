# python3.6

import json
import os
import requests

headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
}

kafka_consumer_group = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumer-group"

kafka_consumer_topic = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumer-group/instances/my-topic-consumer/subscription"

kafka_consumer_records = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumer-group/instances/my-topic-consumer/records"


def create_consumer():
    print("*** 1. Creating Bridge Consumer...")
    data = {}
    data['name'] = 'my-topic-consumer'
    data['auto.offset.reset'] = 'earliest'
    data['format'] = 'json'
    data['enable.auto.commit'] = 'false'
    data['fetch.min.bytes'] = 512
    data['consumer.request.timeout.ms'] = 30000
    json_data = json.dumps(data)

    print("data to kafka:" + json_data)
    response = requests.post(kafka_consumer_group, headers=headers, data=json_data)
    print(response)
    if (response.status_code) == 200:
       subscribe2kafkaTopic()
    else:
       print("error creating consumer", response.txt)
       exit()

# subscribe to kafka topic my-topic
def subscribe2kafkaTopic():
    print("*** 2. Subscribing to topic...")
    data = {}
    data['topics'] = ['my-topic']
    json_data = json.dumps(data)

    print("data to kafka:" + json_data)
    response = requests.post(kafka_consumer_topic, headers=headers, data=json_data)
    print(response)
    if (response.status_code) == 200:
       subscribe2kafkaTopic()
    else:
       print("error creating consumer", response.txt)
       exit()
    
def listenAndProcess():
    response = requests.get(kafka_consumer_records, headers=headers)
    print(response)
    if (response.status_code) == 200:
       print("Successful get!")
    else:
       print("error creating consumer", response.txt)
       exit()
    
def publish2S3(msg):
    print("3. Publishing to AWS S3")

if __name__ == '__main__':
    create_consumer()
    subscribe2KafkaTopic()
    listenAndProcess()
