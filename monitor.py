import os
import requests
import urllib3
import time
import datetime
import argparse

# Silence warning
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

token = os.environ['VARDA_TOKEN']

s = requests.Session()
s.headers = {"Authorization": "Bearer %s" % token}
s.verify = 'nginx-selfsigned.crt'


def monitor(tasks_fn):
    tasks = [line.rstrip('\n') for line in open(tasks_fn)]

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

                cov_norm = (1000000 * running.seconds + running.microseconds) / (diff_stats['coverage'] or 1)
                var_norm = (1000000 * running.seconds + running.microseconds) / ((diff_stats['snv'] + diff_stats['mnv']) or 1)

                print(f'Task {task} is Done. Queuing: {queuing}. Running: {running} Diff_stats: {diff_stats} Cov_norm: {cov_norm} Var_Norm: {var_norm}')

                tasks.remove(task)

        time.sleep(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--tasks-file", required=True,
                    help="Filename of file to store task uuids")
    args = vars(ap.parse_args())

    monitor(tasks_fn=args['tasks_file'])

# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
