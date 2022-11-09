# python3.6

import json
import os
import requests

headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
    'accept': 'application/vnd.kafka.json.v2+json'
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
    import boto3

    #Creating Session With Boto3.
    session = boto3.Session(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
    )

    s3 = session.resource('s3')
    object = s3.Object(aws_s3_bucket, "IoT msg uploaded by RHOCP AMQ Serverless function at the Edge")
    result = object.put(Body=msg)
    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')

if __name__ == '__main__':
    process()

