#!/usr/bin/python3

"""
# get data directly from the queue in JSON format
## curl -u glances:glances -i -H "content-type:application/json" -X POST http://debian:15672/api/queues/glances/glances.cpu/get -d'{"count":50,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'
## curl -u glances:glances -i -H "content-type:application/json" -X POST http://debian:15672/api/queues/glances/glances.cpu/get -d "{""count"":1,""ackmode"":""ack_requeue_true"",""encoding"":""auto"",""truncate"":50000}"'
"""
import csv
import datetime
from datetime import datetime
import json
import requests
from requests.auth import HTTPBasicAuth
import time

headers = {'Content-type': 'application/json'}
data = '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto","truncate":50000}'

while True:
    response = requests.post('http://debian:15672/api/queues/glances/glances.cpu/get', headers=headers, data=data, auth=HTTPBasicAuth('glances', 'glances'))
    if "NOT_FOUND" in response.text:
        print("Message queue is empty.  Sleeping...")
        time.sleep(1)
    else:
        queuedMessage = '{"data":' + response.text + '}'
        queuedMessage = json.loads(queuedMessage)
        with open('cpuData.csv', 'a+', newline='') as f:
            fnames = ['dateinfo', 'user']
            writer = csv.DictWriter(f, fieldnames=fnames)
            for i in queuedMessage["data"]:
                statsdata = (i["payload"])
                stats = statsdata.split(', ')
                statpairs = dict(s.split('=',1) for s in stats)
                writer.writerow({'dateinfo' : datetime.fromisoformat(statpairs['dateinfo']), 'user' : statpairs['user']})
                print("File updated: " + statpairs['user'])
        time.sleep(1)
