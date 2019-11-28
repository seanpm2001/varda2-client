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
            pre_stats = resp.json()['pre_stats']
            post_stats = resp.json()['post_stats']
            diff_stats = {k: post_stats[k] - v for k, v in pre_stats.items()}
            queuing = start - created
            running = end - start

            cov_norm = (1000000 * running.seconds + running.microseconds) / diff_stats['coverage']
            var_norm = (1000000 * running.seconds + running.microseconds) / (diff_stats['snv'] + diff_stats['mnv'])

            print(f'Task {task} is Done. Queuing: {queuing}. Running: {running} Diff_stats: {diff_stats} Cov_norm: {cov_norm} Var_Norm: {var_norm}')

            tasks.remove(task)

    time.sleep(1)
