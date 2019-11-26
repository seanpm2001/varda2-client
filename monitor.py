import os
import requests
import urllib3
import time
import datetime

# Silence warning
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

LAB = 'ACME'
DISEASE_CODE = 'ALS'
token = os.environ['VARDA_TOKEN']

s = requests.Session()
s.headers = {"Authorization": "Bearer %s" % token}
s.verify = 'nginx-selfsigned.crt'

tasks = [line.rstrip('\n') for line in open('tasks.txt')]

while len(tasks):
    for task in tasks:
        resp = s.get('https://res-vard-db01.researchlumc.nl/task/%s' % task)
        state = resp.json()['state']
        if state == 'Done':
            created = datetime.datetime.strptime(resp.json()['created_date'], '%Y-%m-%dT%H:%M:%S.%f')
            start = datetime.datetime.strptime(resp.json()['start_date'], '%Y-%m-%dT%H:%M:%S.%f')
            end = datetime.datetime.strptime(resp.json()['end_date'], '%Y-%m-%dT%H:%M:%S.%f')
            queuing = start - created
            running = end - start

            print(f'Task {task} is Done. Queuing: {queuing}. Running: {running}')
            tasks.remove(task)

    time.sleep(1)
