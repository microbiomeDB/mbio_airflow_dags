import datetime
import pendulum
import csv
import os
import sys

from airflow.models.dag import DAG
from airflow.models.dataset import Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ampliseq",
    schedule=[Dataset("/data/MicrobiomeDB/common/amplicon_sequencing/amplicon_studies.csv")],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    @task
    def process_ampliseq_studies():
        # studies.csv should have two columns, one w the study name and the other the directory
        # we need to make a bash operator for each study

        # we should configure airflow to rerun this if the DAG changes too
        ampliseq_version = "2.9.0"
        base_path = "/data/MicrobiomeDB/common/amplicon_sequencing/"

        # first see whats been run before and under what conditions
        # TODO its possible this info is better in a postgres table
        # though its nice to be able to remove a row to force a rerun
        # and thats easier for most people if its in a csv
        # TODO make a readme file for the base directory that explains
        # its structure and how to use it
        provenance_path = f"{base_path}/processed_studies_provenance.csv"
        with open(provenance_path, 'r') as file:
            # Create a CSV DictReader
            reader = csv.DictReader(file)
    
            # i think we want a dict of dicts
            processed_studies = {}
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 'code_revision': row['code_revision']}

        # get study and path from csv
        studies = []
        paths = []
        all_studies_path = f"{base_path}/amplicon_studies.csv"
        with open(all_studies_path, "r") as file:
            for line in file:
                study, path = line.strip().split(",")
                current_timestamp = os.path.getmtime(path)
                processStudy = False
                if (study in processed_studies):
                    if ((current_timestamp > int(processed_studies[study]['timestamp'])) or
                        (ampliseq_version != processed_studies[study]['code_revision'])):
                        processStudy = True
                else:
                    processStudy = True
                    
                if processStudy: 
                    studies.append(study)
                    paths.append(path)

        config_path = f"{base_path}/ampliseq.config" # TODO validate exists
        
        for i in range(len(studies)):
            study = studies[i]
            study_in_path = paths[i]
            study_params_path = f"{study_in_path}/nf-params.json" # TODO validate exists
            study_out_path = f"{study_in_path}/out"
            task_name = f"ampliseq_{study}"

            nextflow_command = (f"nextflow run nf-core/ampliseq -with-trace "
                                f"-r {ampliseq_version} "
                                f"-c {config_path} "
                                f"--params {study_params_path} "
                                f"--input {study_in_path} "
                                f"--outdir {study_out_path}")
            
            sys.stderr.write(f"running {task_name}\n")
            BashOperator(
                task_id=task_name,
                bash_command=nextflow_command,
                dag="ampliseq"
            )

        # update code revision etc in processed_studies_provenance.csv
        with open(provenance_path, 'w') as file:
            writer = csv.writer(file)
            for study in studies:
                writer.writerow([study, current_timestamp, ampliseq_version])
        
if __name__ == "__main__":
    dag.test()

