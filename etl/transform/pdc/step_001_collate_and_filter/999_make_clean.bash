#!/bin/bash

rm merged_data/pdc/__filtration_logs/*

./001_File.py
./002_Program.py
./003_Project.py
./004_Study.py
./005_Sample.py
./006_Aliquot.py
./007_Demographic.py
./008_Diagnosis.py
./009_Case.py
./010_Reference.py
./011_link_Demographic_and_Diagnosis_to_study_id.py


