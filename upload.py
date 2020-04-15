#!/usr/bin/env python
import os
import requests
import urllib3
import csv
import argparse

token = os.environ['VARDA_TOKEN']
default_server = "varda.lumc.nl"

def upload(var_fn, cov_fn, disease_code, lab_sample_id, task_fn, server):
    s = requests.Session()
    s.headers = {"Authorization": "Bearer %s" % token}

    with open(cov_fn, 'rb') as f:
        payload = {
            "lab_sample_id": lab_sample_id,
            "disease_code": disease_code,
            "type": "coverage",
        }

        try:
            print("Uploading coverage file ...")
            resp = s.post(f'https://{server}/file', data=payload,
                files={"file": f})
            resp.raise_for_status()
            remote_cov_fn = resp.json()["filename"]
            print("done!")
        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)

    with open(var_fn, 'rb') as f:
        payload = {
            "lab_sample_id": lab_sample_id,
            "disease_code": disease_code,
            "type": "variant",
        }

        try:
            print("Uploading variant file ...")
            resp = s.post(f'https://{server}/file', data=payload,
                files={"file": f})
            resp.raise_for_status()
            remote_var_fn = resp.json()["filename"]
            print("done!")
        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)

    try:
        print("Creating sample file ...")
        resp = s.post(f'https://{server}/sample', json={
                'variant_filename': remote_var_fn,
                'coverage_filename': remote_cov_fn,
                'lab_sample_id': lab_sample_id,
                'disease_code': disease_code,
            })
        resp.raise_for_status()
        print("done!")

        # Write task uuid's to file
        fp = open(task_fn, "w")
        fp.write("%s\n" % resp.json()['task'])
        fp.close()

    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--variants", required=True,
            help="Varda variants file")
    ap.add_argument("-c", "--coverage", required=True,
                    help="Varda coverage file")
    ap.add_argument("-d", "--disease-code", required=True,
                    help="Disease indication code")
    ap.add_argument("-l", "--lab-sample-id", required=True,
                    help="Local sample id")
    ap.add_argument("-t", "--task-file", required=True,
                    help="Filename of file to store task uuid")
    ap.add_argument("-s", "--server", required=False, default=default_server,
                    help="Server hostname")
    args = vars(ap.parse_args())

    upload(
            var_fn=args['variants'],
            cov_fn=args['coverage'],
            disease_code=args['disease_code'],
            lab_sample_id=args['lab_sample_id'],
            task_fn=args['task_file'],
            server=args['server'],
    )


# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
