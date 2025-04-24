#!/usr/bin/env bash

################################################################################
# SOURCE URLS

uberon_obo_permanent_url="http://purl.obolibrary.org/obo/uberon/ext.obo"

do_obo_url="https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/refs/heads/main/src/ontology/doid-merged.obo"

icd_o_3_website_url="https://www.naaccr.org/icdo3/"

################################################################################
# OUTPUT TARGETS

ontology_metadata_output_root="./auxiliary_metadata/__ontology_reference"

uberon_output_dir="${ontology_metadata_output_root}/UBERON"

do_output_dir="${ontology_metadata_output_root}/DO"

icd_o_3_output_dir="${ontology_metadata_output_root}/ICD-O-3"

mkdir -p $uberon_output_dir

mkdir -p $do_output_dir

mkdir -p $icd_o_3_output_dir

uberon_obo_file="${uberon_output_dir}/uberon_ext.obo"

do_obo_file="${do_output_dir}/doid-merged.obo"

################################################################################
# DATA FETCHES

echo
echo "[`date`] Downloading UBERON..."
echo

# -L follows links, and is required to get data from github. -o specifies the local output file.

rm -f $uberon_obo_file

echo curl -L -o $uberon_obo_file $uberon_obo_permanent_url
curl -L -o $uberon_obo_file $uberon_obo_permanent_url

release_version=$( grep data-version $uberon_obo_file | sed "s/^.*releases\///" )

new_obo_file_basename="uberon_ext.release_${release_version}.obo"

new_obo_file="${uberon_output_dir}/${new_obo_file_basename}"

echo mv $uberon_obo_file $new_obo_file
mv $uberon_obo_file $new_obo_file

echo ln -sf $new_obo_file_basename $uberon_obo_file
ln -sf $new_obo_file_basename $uberon_obo_file

echo
echo "[`date`] done downloading UBERON."
echo
echo
echo "[`date`] Downloading DO..."
echo

rm -f $do_obo_file

echo curl -L -o $do_obo_file $do_obo_url
curl -L -o $do_obo_file $do_obo_url

release_version=$( grep data-version $do_obo_file | sed "s/^.*releases\///" | sed "s/\/.*$//" )

new_obo_file_basename="doid-merged.release_${release_version}.obo"

new_obo_file="${do_output_dir}/${new_obo_file_basename}"

echo mv $do_obo_file $new_obo_file
mv $do_obo_file $new_obo_file

echo ln -sf $new_obo_file_basename $do_obo_file
ln -sf $new_obo_file_basename $do_obo_file

echo
echo "[`date`] done downloading DO."
echo

################################################################################
# HELPFUL MESSAGE

echo
echo "Please get the WHO IARC ICD-O-3.[latest] Excel table (last seen at ${icd_o_3_website_url}) and export it to ${ontology_metadata_output_root}/ICD-O-3/icd_morphology.tsv before proceeding."
echo
echo


