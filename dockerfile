FROM apache/airflow:2.2.5
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
