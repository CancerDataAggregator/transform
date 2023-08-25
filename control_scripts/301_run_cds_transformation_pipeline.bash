#!/usr/bin/env bash

chmod 755 package_root/transform/cds/scripts/*.py

echo ./package_root/transform/cds/scripts/001_File.py
./package_root/transform/cds/scripts/001_File.py

echo ./package_root/transform/cds/scripts/002_ResearchSubject.Diagnosis.Specimen.Subject.py
./package_root/transform/cds/scripts/002_ResearchSubject.Diagnosis.Specimen.Subject.py


