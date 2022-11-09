# python3.6

import json
import os
import requests
import boto3
import datetime
from datetime import datetime, date

yyyy-mm-dd = str(date.today())
now = datetime.datetime.now()

aws_access_key_id = "AKIA4VEYXFSR7QSUPHMF"
aws_secret_access_key = "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"
aws_s3_bucket = "rhel9-820-homelabs-iot-sensor"

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
    publish2S3(response.text)
    
def publish2S3(msg):
    print("4. Publishing to AWS S3")
    import boto3

    #Creating Session With Boto3.
    session = boto3.Session(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
    )

    s3 = session.resource('s3')
    
    prefix = 'folder_' + yyyy-mm-dd + "/" + now
    print("AWS S3 bucket folder and file: ", prefix)

    object = s3.Object(aws_s3_bucket, prefix)
    result = object.put(Body=msg)
    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')

#main
if __name__ == '__main__':
    process()
