from string import Template


def query_all_cases():
    query = """{
  allCases(acceptDUA: true) {
    case_id
  }
}"""
    return query


def query_single_case(case_id):
    template = Template(
        """{
  case(case_id: "$case_id" acceptDUA: true) {
    case_id
    case_submitter_id
    disease_type
    primary_site
    project_submitter_id
    demographics {
      ethnicity
      gender
      race
      days_to_birth
    }
    samples {
      sample_id
      sample_submitter_id
      biospecimen_anatomic_site
      sample_type
    }
    diagnoses {
      age_at_diagnosis
      diagnosis_id
      tumor_grade
      tumor_stage
      morphology
      primary_diagnosis
    }
  }
}
"""
    )

    query = template.substitute(case_id=case_id)
    return query


def query_files_bulk(offset, limit):
    template = Template(
        """{
  filesMetadata(offset: $offset limit: $limit acceptDUA: true) {
    file_id
    file_name
    file_location
    md5sum
    file_size
    file_submitter_id
    data_category
    file_type
    file_format
    experiment_type
    aliquots {
      sample_id
    }
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query

def query_files_paginated(offset, limit):
    template = Template(
        """{
  getPaginatedFiles(offset: $offset limit: $limit acceptDUA: true) {
    total
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query
