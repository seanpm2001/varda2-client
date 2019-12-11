import os
import requests
import urllib3
import csv

# Silence warning
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

token = os.environ['VARDA_TOKEN']

s = requests.Session()
s.headers = {"Authorization": "Bearer %s" % token}
s.verify = 'nginx-selfsigned.crt'

endpoints = ['coverage', 'seq', 'snv', 'mnv']
for endpoint in endpoints:
    resp = s.post(f'https://res-vard-db01.researchlumc.nl/{endpoint}/save')
