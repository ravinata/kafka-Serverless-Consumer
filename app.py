# python3.6

import json
import os
import requests

headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
}

kafka_consumer_group = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumerX-group"

kafka_consumer_topic = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumerX-group/instances/my-topic-consumerX/subscription"

kafka_consumer_records = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumerX-group/instances/my-topic-consumerX/records"


def process():
    print("*** 1. Creating Bridge Consumer Group...")
    data = {}
    data['name'] = 'my-topic-consumerX'
    data['auto.offset.reset'] = 'earliest'
    data['format'] = 'json'
    data['fetch.min.bytes'] = 512
    data['consumer.request.timeout.ms'] = 30000
    json_data = json.dumps(data)

    print("data to kafka:" + json_data)
    response = requests.post(kafka_consumer_group, headers=headers, data=json_data)
    print(response.text)
    subscribe2kafkaTopic()

# subscribe to kafka topic my-topic
def subscribe2kafkaTopic():
    print("*** 2. Subscribing to topic...")
    data = {}
    data['topics'] = ['my-topic']
    json_data = json.dumps(data)

    print("data to kafka:" + json_data)
    response = requests.post(kafka_consumer_topic, headers=headers, data=json_data)
    print(response.text)
    listenAndProcess()
    
def listenAndProcess():
    print("*** 3. Fetching Records... ")
    response = requests.get(kafka_consumer_records, headers=headers)
    print(response.text)
    
def publish2S3(msg):
    print("4. Publishing to AWS S3")
    print("*** yet to implement ***")

if __name__ == '__main__':
    process()

