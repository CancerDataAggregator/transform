FROM python:3.8
ENV GIT_CDA_ETL_BRANCH rm_files_nesting_structs
WORKDIR /cda_repos/transform
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install git+https://github.com/CancerDataAggregator/cda-python.git@{GIT_CDA_ETL_BRANCH}