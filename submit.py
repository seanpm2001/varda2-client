import os
import requests
import urllib3
import csv
import argparse

# Silence warning
urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)

token = os.environ['VARDA_TOKEN']

def submit(samplesheet_fn, tasks_fn, lab_name, disease_code):
    s = requests.Session()
    s.headers = {"Authorization": "Bearer %s" % token}
    s.verify = 'nginx-selfsigned.crt'

    # Get sample id's from file
    sample_ids = []
    with open(samplesheet_fn, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ')
        for row in reader:
            sample_ids.append(row[0])

    # Assuming the files are already in the uploads directory remotely, create and submit samples
    tasks = []
    for sample_id in sample_ids[:50]:
        var_file = '%s_variants.varda' % sample_id
        cov_file = '%s_coverage.varda' % sample_id
        print('%s %s %s' % (sample_id, var_file, cov_file))

        resp = s.post('https://res-vard-db01.researchlumc.nl/sample', json={
            'variant_filename': var_file,
            'coverage_filename': cov_file,
            'lab': lab_name,
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-s", "--sample-sheet", required=True,
            help="Sample sheet file: sample_id, gvcf, vcf, bam")
    ap.add_argument("-t", "--tasks-file", required=True,
                    help="Filename of file to store task uuids")
    ap.add_argument("-l", "--lab-name", required=True,
                    help="Name of the lab")
    ap.add_argument("-d", "--disease-code", required=True,
                    help="Name of the lab")
    args = vars(ap.parse_args())

    submit(
            samplesheet_fn=args['sample_sheet'],
            tasks_fn=args['tasks_file'],
            lab_name=args['lab_name'],
            disease_code=args['disease_code']
    )


# Entry point of we are called as standalone script
if __name__ == "__main__":
    main()
