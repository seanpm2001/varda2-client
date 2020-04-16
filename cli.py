#!/usr/bin/env python
import argparse
import csv
import datetime
import os
import requests
import time
import urllib3

default_server = "varda.lumc.nl"
token = os.environ['VARDA_TOKEN']


def annotate(tasks_fn, samplesheet_fn, session, server):

    # Get sample id's from file
    sample_ids = []
    with open(samplesheet_fn, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ')
        for row in reader:
            sample_ids.append(row[0])

    # Assuming the files are already in the uploads directory remotely, create and submit samples
    tasks = []
    for sample_id in sample_ids[0:]:
        var_file = '%s_variants.varda' % sample_id
        print('%s %s' % (sample_id, var_file))

        resp = session.post(f'https://{server}/annotation', json={
            'variant_filename': var_file,
            'lab_sample_id': sample_id,
        })
        print(resp.json())
        task_uuid = resp.json()['task']

        # Record tasks
        tasks.append(task_uuid)


    # Write task uuid's to file
    fp = open(tasks_fn, "w")
    for task in tasks:
        fp.write("%s\n" % task)
    fp.close()


def stab(session, server, reference, start, end):

    try:
        print(f"Stab query ...")
        resp = session.post(f'https://{server}/coverage/query_stab', json={
            "reference_seq_id": reference,
            "start": start,
            "end": end,
        })
        resp.raise_for_status()
        print("done!")
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def version(session, server):

    try:
        print(f"Retrieving version ...")
        resp = session.get(f'https://{server}/version')
        resp.raise_for_status()
        print("done!")
        ver = resp.json()
        print("%d.%d.%d" % (ver["major"], ver["minor"], ver["patch"]))
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def save(session, server):

    endpoints = ['coverage', 'seq', 'snv', 'mnv']
    for endpoint in endpoints:
        try:
            print(f"Saving {endpoint} table ...")
            resp = session.post(f'https://{server}/{endpoint}/save')
            resp.raise_for_status()
            print("done!")
        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)


def submit(samplesheet_fn, tasks_fn, disease_code, session, server):

    # Get sample id's from file
    sample_ids = []
    with open(samplesheet_fn, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ')
        for row in reader:
            sample_ids.append(row[0])

    # Assuming the files are already in the uploads directory remotely, create and submit samples
    tasks = []
    for sample_id in sample_ids:
        var_file = '%s_variants.varda' % sample_id
        cov_file = '%s_coverage.varda' % sample_id
        print('%s %s %s' % (sample_id, var_file, cov_file))

        resp = session.post(f'https://{server}/sample', json={
            'variant_filename': var_file,
            'coverage_filename': cov_file,
            'lab_sample_id': sample_id,
            'disease_code': disease_code,
        })
        task_uuid = resp.json()['task']

        # Record tasks
        tasks.append(task_uuid)

    # Write task uuid's to file
    fp = open(tasks_fn, "w")
    for task in tasks:
        fp.write("%s\n" % task)
    fp.close()


def monitor(tasks_fn, server, session):
    tasks = [line.rstrip('\n') for line in open(tasks_fn)]

    while len(tasks):
        for task in tasks:
            resp = session.get(f'https://{server}/task/%s' % task)
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

                print(f'Task {task} is Done. Queuing: {queuing}. Running: {running} Diff_stats: {diff_stats}')

                tasks.remove(task)

        time.sleep(1)


def upload(var_fn, cov_fn, disease_code, lab_sample_id, task_fn, server, session):

    with open(cov_fn, 'rb') as f:
        payload = {
            "lab_sample_id": lab_sample_id,
            "disease_code": disease_code,
            "type": "coverage",
        }

        try:
            print("Uploading coverage file ...")
            resp = session.post(f'https://{server}/file', data=payload,
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
            resp = session.post(f'https://{server}/file', data=payload,
                          files={"file": f})
            resp.raise_for_status()
            remote_var_fn = resp.json()["filename"]
            print("done!")
        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)

    try:
        print("Creating sample file ...")
        resp = session.post(f'https://{server}/sample', json={
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
    subparsers = ap.add_subparsers()
    ap.add_argument("-s", "--server", required=False, default=default_server, help="Server hostname")
    ap.add_argument("-c", "--certificate", required=False, help="Certificate")

    #
    # Upload subcommand
    #
    upload_parser = subparsers.add_parser('upload', help='Upload files to varda server')
    upload_parser.add_argument("-v", "--variants-file", required=True, dest="var_fn",
                    help="Varda variants file")
    upload_parser.add_argument("-c", "--coverage", required=True, dest="cov_fn",
                    help="Varda coverage file")
    upload_parser.add_argument("-d", "--disease-code", required=True,
                    help="Disease indication code")
    upload_parser.add_argument("-l", "--lab-sample-id", required=True,
                    help="Local sample id")
    upload_parser.add_argument("-t", "--task-file", required=True, dest="task_fn",
                    help="Filename of file to store task uuid")
    upload_parser.set_defaults(func=upload)

    #
    # Monitor subcommand
    #
    monitor_parser = subparsers.add_parser('monitor', help='Monitor tasks')
    monitor_parser.add_argument("-t", "--task-file", required=True, help="Filename of tasks to monitor", dest="tasks_fn")
    monitor_parser.set_defaults(func=monitor)

    #
    # Submit subcommand
    #
    submit_parser = subparsers.add_parser('submit', help='Submit without upload')
    submit_parser.add_argument("-s", "--sample-sheet", required=True, dest="samplesheet_fn",
                    help="Sample sheet file: sample_id, gvcf, vcf, bam")
    submit_parser.add_argument("-t", "--tasks-file", required=True, dest="tasks_fn",
                    help="Filename of file to store task uuids")
    submit_parser.add_argument("-d", "--disease-code", required=True,
                    help="Disease indication code")
    submit_parser.set_defaults(func=submit)

    #
    # Save subcommand
    #
    save_parser = subparsers.add_parser('save', help='Save tables')
    save_parser.set_defaults(func=save)

    #
    # version subcommand
    #
    version_parser = subparsers.add_parser('version', help='Retrieve version')
    version_parser.set_defaults(func=version)

    #
    # annotate subcommand
    #
    annotate_parser = subparsers.add_parser('annotate', help='annotate file')
    annotate_parser.set_defaults(func=annotate)
    annotate_parser.add_argument("-s", "--sample-sheet", required=True, dest="samplesheet_fn",
                               help="Sample sheet file: sample_id, gvcf, vcf, bam")
    annotate_parser.add_argument("-t", "--tasks-file", required=True, dest="tasks_fn",
                               help="Filename of file to store task uuids")

    #
    # stab subcommand
    #
    stab_parser = subparsers.add_parser('stab', help='Stab query')
    stab_parser.add_argument("-s", "--start", required=True, help="Start of region")
    stab_parser.add_argument("-e", "--end", required=True, help="End of region")
    stab_parser.add_argument("-r", "--reference", required=True, help="Chromosome to look at")
    stab_parser.set_defaults(func=stab)

    #
    # END OF SUB-COMMANDS
    #

    #
    # Initialize session
    #
    session = requests.Session()
    session.headers = {"Authorization": "Bearer %s" % token}
    ap.set_defaults(session=session)

    # Process arguments and put into dict
    args = vars(ap.parse_args())

    # Specify custom cert for self-signed server certificate
    certificate = args.pop("certificate", None)
    if certificate:
        print("use certificate")
        session.verify = certificate
        # Silence warning
        urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

    # Don't want to pass "func" as an argument
    func = args.pop('func', None)
    if func:
        func(**args)


# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
