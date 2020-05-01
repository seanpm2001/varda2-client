#!/usr/bin/env python
import argparse
import csv
import datetime
import os
import pprint
import requests
import time
import urllib3

default_server = "varda.lumc.nl"
token = os.environ['VARDA_TOKEN']


def submit(samplesheet_fn, var_fn, cov_fn, disease_code, lab_sample_id, tasks_fn, server, session):

    assert not (samplesheet_fn and var_fn)

    triples = []
    if samplesheet_fn:

        # Get sample id's from file
        sample_ids = []
        with open(samplesheet_fn, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ')
            for row in reader:
                sample_id = row[0]
                var_file = '%s_variants.varda' % sample_id
                cov_file = '%s_coverage.varda' % sample_id
                triples.append((sample_id, var_file, cov_file))
    else:
        print("Uploading coverage file ...")
        remote_cov_fn = upload_helper(session, server, cov_fn, lab_sample_id, disease_code, "coverage")
        print("Uploading variant file ...")
        remote_var_fn = upload_helper(session, server, var_fn, lab_sample_id, disease_code, "variant")
        triples.append((lab_sample_id, remote_var_fn, remote_cov_fn))

    tasks = []
    for pair in triples:
        try:
            print("Creating sample entry ...")
            resp = session.post(f'https://{server}/sample', json={
                'lab_sample_id': pair[0],
                'variant_filename': pair[1],
                'coverage_filename': pair[2],
                'disease_code': disease_code,
            })
            resp.raise_for_status()
            print("done!")

        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)

        # Record tasks
        task_uuid = resp.json()['task']
        tasks.append(task_uuid)

    # Write task uuid's to file
    fp = open(tasks_fn, "w")
    for t in tasks:
        fp.write("%s\n" % t)
    fp.close()


def annotate(tasks_fn, samplesheet_fn, var_fn, session, server, lab_sample_id):

    assert not (samplesheet_fn and var_fn)

    tuples = []
    if samplesheet_fn:

        # Get sample id's from file
        with open(samplesheet_fn, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ')
            for row in reader:
                sample_id = row[0]
                var_file = '%s_variants.varda' % sample_id
                tuples.append((sample_id, var_file))

    else:
        print("Uploading variant file ...")
        remote_variant_fn = upload_helper(session, server, var_fn, lab_sample_id, "N/A", "variant")
        tuples.append((lab_sample_id, remote_variant_fn))

    # Assuming the files are already in the uploads directory remotely, create and submit samples
    tasks = []
    for pair in tuples:
        resp = session.post(f'https://{server}/annotation', json={
            'lab_sample_id': pair[0],
            'variant_filename': pair[1],
        })
        print(resp.json())
        task_uuid = resp.json()['task']

        # Record tasks
        tasks.append(task_uuid)

    # Write task uuid's to file
    fp = open(tasks_fn, "w")
    for t in tasks:
        fp.write("%s\n" % t)
    fp.close()


def task(session, server, uuid):

    try:
        print(f"Task query ...")
        resp = session.get(f'https://{server}/task/{uuid}')
        resp.raise_for_status()
        print("done!")
        pprint.pprint(resp.json())
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def sample(session, server, uuid):

    try:
        print(f"Sample query ...")
        resp = session.get(f'https://{server}/sample/{uuid}')
        resp.raise_for_status()
        print("done!")
        pprint.pprint(resp.json())
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def seq(session, server, sequence):

    try:
        print(f"Sequence query ...")
        resp = session.post(f'https://{server}/seq/query', json={
            "inserted_seq": sequence,
        })
        resp.raise_for_status()
        print("done!")
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def snv(session, server, reference, position, inserted):

    try:
        print(f"SNV query ...")
        resp = session.post(f'https://{server}/snv/query', json={
            "inserted": inserted,
            "reference_seq_id": reference,
            "position": position,
        })
        resp.raise_for_status()
        print("done!")
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


def mnv(session, server, reference, start, end, inserted):

    try:
        print(f"MNV query ...")
        resp = session.post(f'https://{server}/mnv/query', json={
            "start": start,
            "end": end,
            "inserted_seq": inserted,
            "reference_seq_id": reference,
        })
        resp.raise_for_status()
        print("done!")
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        print("failed!")
        raise SystemExit(err)


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


def upload_helper(session, server, filename, lab_sample_id, disease_code, file_type):

    with open(filename, 'rb') as f:
        payload = {
            "lab_sample_id": lab_sample_id,
            "disease_code": disease_code,
            "type": file_type,
        }

        try:
            resp = session.post(f'https://{server}/file', data=payload,
                                files={"file": f})
            print(resp.json())
            resp.raise_for_status()
            remote_var_fn = resp.json()["filename"]
            print("done!")
        except requests.exceptions.HTTPError as err:
            print("failed!")
            raise SystemExit(err)

    return remote_var_fn


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser.add_argument("-s", "--server", required=False, default=default_server, help="Server hostname")
    parser.add_argument("-c", "--certificate", required=False, help="Certificate")

    #
    # Submit subcommand
    #
    submit_parser = subparsers.add_parser('submit', help='Submit without upload')
    submit_parser.set_defaults(func=submit)
    submit_parser.add_argument("-t", "--tasks-file", required=True, dest="tasks_fn",
                               help="Filename of file to store task uuids")
    submit_parser.add_argument("-d", "--disease-code", required=True,
                               help="Disease indication code")
    submit_parser.add_argument("-l", "--lab-sample-id", required=False,
                               help="Local sample id")
    group = submit_parser.add_mutually_exclusive_group(required=True)
    submit_parser.add_argument("-c", "--coverage", required=False, dest="cov_fn",
                               help="Varda coverage file")
    group.add_argument("-s", "--sample-sheet", required=False, dest="samplesheet_fn",
                       help="Sample sheet file: sample_id, gvcf, vcf, bam")
    group.add_argument("-v", "--variants-file", required=False, dest="var_fn",
                       help="Varda variants file")

    #
    # Monitor subcommand
    #
    monitor_parser = subparsers.add_parser('monitor', help='Monitor tasks')
    monitor_parser.add_argument("-t", "--task-file", required=True, help="Filename of tasks to monitor", dest="tasks_fn")
    monitor_parser.set_defaults(func=monitor)


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
    annotate_parser = subparsers.add_parser('annotate', help='Annotate file(s) with optional upload')
    annotate_parser.set_defaults(func=annotate)
    annotate_parser.add_argument("-t", "--tasks-file", required=True, dest="tasks_fn",
                                 help="Filename of file to store task uuids")
    group = annotate_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--sample-sheet", required=False, dest="samplesheet_fn",
                       help="Sample sheet file: sample_id, gvcf, vcf, bam")
    group.add_argument("-v", "--variants-file", required=False, dest="var_fn",
                       help="Varda variants file")
    annotate_parser.add_argument("-l", "--lab-sample-id", required=False, default="N/A",
                                 help="Local sample id")

    #
    # stab subcommand
    #
    stab_parser = subparsers.add_parser('stab', help='Stab query')
    stab_parser.add_argument("-s", "--start", required=True, help="Start of region")
    stab_parser.add_argument("-e", "--end", required=True, help="End of region")
    stab_parser.add_argument("-r", "--reference", required=True, help="Chromosome to look at")
    stab_parser.set_defaults(func=stab)

    #
    # sequence query subcommand
    #
    seq_parser = subparsers.add_parser('seq', help='Sequence query')
    seq_parser.add_argument("-s", "--sequence", required=True, help="Sequence")
    seq_parser.set_defaults(func=seq)

    #
    # snv query subcommand
    #
    snv_parser = subparsers.add_parser('snv', help='SNV query')
    snv_parser.add_argument("-p", "--position", required=True, help="Locus position")
    snv_parser.add_argument("-i", "--inserted", required=True, help="Inserted base")
    snv_parser.add_argument("-r", "--reference", required=True, help="Chromosome to look at")
    snv_parser.set_defaults(func=snv)

    #
    # mnv query subcommand
    #
    mnv_parser = subparsers.add_parser('mnv', help='MNV query')
    mnv_parser.add_argument("-s", "--start", required=True, help="Start of region")
    mnv_parser.add_argument("-e", "--end", required=True, help="End of region")
    mnv_parser.add_argument("-i", "--inserted", required=True, help="Inserted sequence")
    mnv_parser.add_argument("-r", "--reference", required=True, help="Chromosome to look at")
    mnv_parser.set_defaults(func=mnv)

    #
    # task query subcommand
    #
    task_parser = subparsers.add_parser('task', help='Task query')
    task_parser.add_argument("-u", "--uuid", required=True, help="Task UUID")
    task_parser.set_defaults(func=task)

    #
    # sample query subcommand
    #
    sample_parser = subparsers.add_parser('sample', help='Sample query')
    sample_parser.add_argument("-u", "--uuid", required=True, help="Sample UUID")
    sample_parser.set_defaults(func=sample)

    #
    # END OF SUB-COMMANDS
    #

    #
    # Initialize session
    #
    session = requests.Session()
    session.headers = {"Authorization": "Bearer %s" % token}
    parser.set_defaults(session=session)

    # Process arguments and put into dict
    raw_args = parser.parse_args()
    args = vars(raw_args)

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
        print(args)
        if func == submit and not args['samplesheet_fn'] and \
                not all(args[x] is not None for x in ['var_fn', 'cov_fn', 'lab_sample_id', 'disease_code']):
            parser.error('--variants-file, --coverage-file, --lab_sample_id and --disease_code must be given together')

        func(**args)
    else:
        parser.print_help()


# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
