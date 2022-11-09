# python3.6

import json
import os
import requests

headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
}
kafka_server = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080"
topic = "my-topic"

def create_consumer():
    

# subscribe to kafka topic my-topic
def subscribe2kafkaTopic():
    response = requests.post(kafka_server, headers=headers, data=data)
    print(response)

def publish2S3(msg):
    print("publishing to AWS S3")

if __name__ == '__main__':
    create_consumer()
    subscribe2KafkaTopic()
    
    
''' for reference
    echo "1.creating bridge customer..."
curl -X POST "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumer-group" \
  -H 'content-type: application/vnd.kafka.v2+json' \
  -d '{
    "name": "my-topic-consumer",
    "auto.offset.reset": "earliest",
    "format": "json",
    "enable.auto.commit": false,
    "fetch.min.bytes": 512,
    "consumer.request.timeout.ms": 30000
  }'

echo "2.subscribe to the topic as a customer..."
curl -X POST http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/consumers/my-topic-consumer-group/instances/my-topic-consumer/subscription \
  -H 'content-type: application/vnd.kafka.v2+json' \
  -d '{
    "topics": [
        "my-topic"
    ]
}'
'''
    
