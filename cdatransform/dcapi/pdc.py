import json
import jsonlines
import time
import sys
import gzip
import argparse

from .lib import get_case_ids, retry_get


def query(case_id):
    return (
        "{"
        f'case(case_id: "{case_id}")'
        """
{
    case_submitter_id
    case_id
    project_submitter_id
    disease_type
    primary_site
    samples {
        sample_id 
        sample_type 
        sample_submitter_id 
        biospecimen_anatomic_site
    }
    demographics {
        ethnicity 
        gender 
        race 
        days_to_birth
    }
    diagnoses {
        tumor_grade
        tumor_stage
    } 
}
"""
        "}"
    )


class PDC:
    def __init__(self, endpoint="https://pdc.cancer.gov/graphql") -> None:
        self.endpoint = endpoint

    def cases(
        self,
        case_ids=None,
    ):

        if case_ids is None:
            case_ids = self.get_case_id_list()

        for case_id in case_ids:
            result = retry_get(self.endpoint, params={"query": query(case_id=case_id)})
            yield result.json()["data"]["case"][0]

    def get_case_id_list(self):
        params = {"query": "{allCases {case_id}}"}
        result = retry_get(self.endpoint, params=params)
        for case in result.json()["data"]["allCases"]:
            yield case["case_id"]

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, 'wb') as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")


def main():
    parser = argparse.ArgumentParser(description='Pull case data from PDC API.')
    parser.add_argument('out_file', help='Out file name. Should end with .gz')
    parser.add_argument('--cases', help='Optional file with list of case ids (one to a line)')    
    args = parser.parse_args()

    pdc = PDC()
    pdc.save_cases(args.out_file, case_ids=get_case_ids(args.cases))


if __name__ == "__main__":
    main()
