# python3.6

import json
import os
import requests

headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
}
kafka_server = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/topics/my-topic"
topic = "my-topic"


# subscribe to kafka topic my-topic
def subscribe2kafkaTopic():
    msg = imsg.replace('"', "\'")
    a = '{"records": [{"key": "my-key", "value": '
    b = '"' + msg + '"'
    c = " }]}"
    data = a + b + c 
    print("data to kafka:" + data)
    response = requests.post(kafka_server, headers=headers, data=data)
    print(response)

def publish2S3(msg):
    print("publishing to AWS S3")

if __name__ == '__main__':
    run()
