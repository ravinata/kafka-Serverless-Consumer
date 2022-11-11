# python3.6

import json
import os
import requests
import boto3
import datetime, time
from datetime import datetime, date

dateTimeObj  = datetime.now()
dateStr      = dateTimeObj.strftime("%d-%b-%Y")
timestampStr = dateTimeObj.strftime("%H:%M:%S.%f")
prefix       = dateStr  + "/" + timestampStr

aws_access_key_id     = "AKIA4VEYXFSR7QSUPHMF"
aws_secret_access_key = "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"
aws_s3_bucket         = "rhel9-820-homelabs-iot-sensor"

server = "http://my-bridge-bridge-service.openshift-operators.svc.cluster.local:8080/topics/my-topic"

def process():
    count = 0
    while True:
       response = requests.get(server)
       resp_json = response.json()
       for msg in resp_json.items():
           print("msg received. Publishing to AWS S3...")
           print(msg)
           publish2S3(msg)
           count=+1
       time.sleep(3600)
       # just so that this pod is not always running
       if(count >= 100):
         exit()
    
def publish2S3(msg):
    print("4. Publishing to AWS S3")
    import boto3

    #Creating Session With Boto3.
    session = boto3.Session(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
    )

    s3 = session.resource('s3')
    
    prefix =  dateStr  + "/" + timestampStr
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
