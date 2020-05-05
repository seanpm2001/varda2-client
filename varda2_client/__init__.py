#!/usr/bin/env python
import argparse
import csv
import datetime
import os
import pprint
import requests
import time
import urllib3
import sys
import json

default_server = "varda.lumc.nl"
default_proto = "https"
token_env = "VARDA_TOKEN"


def submit(samplesheet_fn, var_fn, cov_fn, disease_code, lab_sample_id, proto, server, session, verbose):

    assert not (samplesheet_fn and var_fn)

    triples = []
    if samplesheet_fn:

        with open(samplesheet_fn, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ')
            for row in reader:
                sample_id = row[0]
                var_file = '%s_variants.varda' % sample_id
                cov_file = '%s_coverage.varda' % sample_id
                triples.append((sample_id, var_file, cov_file))
    else:
        if verbose:
            print("Uploading coverage file ...", file=sys.stderr)
        remote_cov_fn = upload_helper(session, proto, server, cov_fn, lab_sample_id, disease_code, "coverage", verbose)
        if verbose:
            print("Uploading variant file ...", file=sys.stderr)
        remote_var_fn = upload_helper(session, proto, server, var_fn, lab_sample_id, disease_code, "variant", verbose)
        triples.append((lab_sample_id, remote_var_fn, remote_cov_fn))

    responses = {}
    for entry in triples:
        lab_sample_id = entry[0]
        variant_filename = entry[1]
        coverage_filename = entry[2]
        try:
            if verbose:
                print("Creating sample entry ...", file=sys.stderr)
            resp = session.post(f'{proto}://{server}/sample', json={
                'lab_sample_id': lab_sample_id,
                'variant_filename': variant_filename,
                'coverage_filename': coverage_filename,
                'disease_code': disease_code,
            })
            resp.raise_for_status()
            if verbose:
                print("done!", file=sys.stderr)

        except requests.exceptions.HTTPError as err:
            if verbose:
                print("failed!", file=sys.stderr)
            raise SystemExit(err)

        # Record response
        responses[lab_sample_id] = resp.json()

    print(json.dumps(responses))


def annotate(samplesheet_fn, var_fn, session, proto, server, lab_sample_id, verbose):

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
        if verbose:
            print("Uploading variant file ...", file=sys.stderr)
        remote_variant_fn = upload_helper(session, proto, server, var_fn, lab_sample_id, "N/A", "variant", verbose)
        tuples.append((lab_sample_id, remote_variant_fn))

    # Assuming the files are already in the uploads directory remotely, create and submit samples
    responses = {}
    for pair in tuples:
        resp = session.post(f'{proto}://{server}/annotation', json={
            'lab_sample_id': pair[0],
            'variant_filename': pair[1],
        })
        # Record responses
        responses[pair[0]] = resp.json()

    print(json.dumps(responses))


def task(session, proto, server, uuid, verbose):

    try:
        if verbose:
            print(f"Task query ...", file=sys.stderr)
        resp = session.get(f'{proto}://{server}/task/{uuid}')
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        pprint.pprint(resp.json())
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def sample(session, proto, server, uuid, verbose, disease_code, lab_sample_id):

    try:
        if verbose:
            print(f"Sample query ...", file=sys.stderr)

        if not (disease_code or lab_sample_id):
            resp = session.get(f'{proto}://{server}/sample/{uuid}')
        else:
            payload = {}
            if disease_code:
                payload.update({
                    "disease_code": disease_code,
                })
            if lab_sample_id:
                payload.update({
                    "lab_sample_id": lab_sample_id
                })
            resp = session.patch(f'{proto}://{server}/sample/{uuid}', json=payload)

        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        pprint.pprint(resp.json())
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def seq(session, proto, server, sequence, verbose):

    try:
        if verbose:
            print(f"Sequence query ...", file=sys.stderr)
        resp = session.post(f'{proto}://{server}/seq/query', json={
            "inserted_seq": sequence,
        })
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def snv(session, proto, server, reference, position, inserted, verbose):

    try:
        if verbose:
            print(f"SNV query ...", file=sys.stderr)
        resp = session.post(f'{proto}://{server}/snv/query', json={
            "inserted": inserted,
            "reference_seq_id": reference,
            "position": position,
        })
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def mnv(session, proto, server, reference, start, end, inserted, verbose):

    try:
        if verbose:
            print(f"MNV query ...", file=sys.stderr)
        resp = session.post(f'{proto}://{server}/mnv/query', json={
            "start": start,
            "end": end,
            "inserted_seq": inserted,
            "reference_seq_id": reference,
        })
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def stab(session, proto, server, reference, start, end, verbose):

    try:
        if verbose:
            print(f"Stab query ...", file=sys.stderr)
        resp = session.post(f'{proto}://{server}/coverage/query_stab', json={
            "reference_seq_id": reference,
            "start": start,
            "end": end,
        })
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        print(resp.json()["message"])
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def version(session, proto, server, verbose):

    try:
        if verbose:
            print(f"Retrieving version ...", file=sys.stderr)
        resp = session.get(f'{proto}://{server}/version')
        resp.raise_for_status()
        if verbose:
            print("done!", file=sys.stderr)
        ver = resp.json()
        print("%d.%d.%d" % (ver["major"], ver["minor"], ver["patch"]))
    except requests.exceptions.HTTPError as err:
        if verbose:
            print("failed!", file=sys.stderr)
        raise SystemExit(err)


def save(session, proto, server, verbose):

    endpoints = ['coverage', 'seq', 'snv', 'mnv']
    for endpoint in endpoints:
        try:
            if verbose:
                print(f"Saving {endpoint} table ...", file=sys.stderr)
            resp = session.post(f'{proto}://{server}/{endpoint}/save')
            resp.raise_for_status()
            if verbose:
                print("done!", file=sys.stderr)
        except requests.exceptions.HTTPError as err:
            if verbose:
                print("failed!", file=sys.stderr)
            raise SystemExit(err)


def monitor(tasks_fn, proto, server, session, verbose):
    tasks = [line.rstrip('\n') for line in open(tasks_fn)]

    while len(tasks):
        for task in tasks:
            resp = session.get(f'{proto}://{server}/task/%s' % task)
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


def upload_helper(session, proto, server, filename, lab_sample_id, disease_code, file_type, verbose):

    with open(filename, 'rb') as f:
        payload = {
            "lab_sample_id": lab_sample_id,
            "disease_code": disease_code,
            "type": file_type,
        }

        try:
            resp = session.post(f'{proto}://{server}/file', data=payload,
                                files={"file": f})
            resp.raise_for_status()
            remote_var_fn = resp.json()["filename"]
            if verbose:
                print("done!", file=sys.stderr)
        except requests.exceptions.HTTPError as err:
            if verbose:
                print("failed!", file=sys.stderr)
            raise SystemExit(err)

    return remote_var_fn


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser.add_argument("-p", "--protocol", required=False, default=default_proto, help="Server protocol", dest="proto", choices=["http", "https"])
    parser.add_argument("-s", "--server", required=False, default=default_server, help="Server hostname")
    parser.add_argument("-c", "--certificate", required=False, help="Certificate")
    parser.add_argument("-v", "--verbose", required=False, help="Verbose output", default=False, action='store_true')

    #
    # Submit subcommand
    #
    submit_parser = subparsers.add_parser('submit', help='Submit new sample')
    submit_parser.set_defaults(func=submit)
    submit_parser.add_argument("-d", "--disease-code", required=True,
                               help="Disease indication code")
    submit_parser.add_argument("-l", "--lab-sample-id", required=False,
                               help="Local sample id")
    submit_parser.add_argument("-c", "--coverage-file", required=False, dest="cov_fn",
                               help="Varda coverage file")
    group = submit_parser.add_mutually_exclusive_group(required=True)
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
    sample_parser.add_argument("-d", "--disease-code", required=False, help="Disease indication code")
    sample_parser.add_argument("-l", "--lab-sample-id", required=False, help="Local sample id")
    sample_parser.set_defaults(func=sample)

    #
    # END OF SUB-COMMANDS
    #

    #
    # Initialize session
    #
    session = requests.Session()
    parser.set_defaults(session=session)

    #
    # Process arguments and put into dict
    #
    raw_args = parser.parse_args()
    args = vars(raw_args)

    #
    # Get token and add to header
    #
    try:
        token = os.environ[token_env]
    except KeyError:
        print(f"Put API token in {token_env} environment variable.")
        sys.exit(1)
    session.headers = {"Authorization": "Bearer %s" % token}

    # Specify custom cert for self-signed server certificate
    certificate = args.pop("certificate", None)
    if certificate:
        session.verify = certificate
        # Silence warning
        urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

    # Don't want to pass "func" as an argument
    func = args.pop('func', None)

    if func:
        if func == submit and not args['samplesheet_fn'] and \
                not all(args[x] is not None for x in ['var_fn', 'cov_fn', 'lab_sample_id', 'disease_code']):
            parser.error('--variants-file, --coverage-file, --lab-sample-id and --disease-code must be given together')

        func(**args)
    else:
        parser.print_help()


# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
