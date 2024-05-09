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
    dag_id="automated_ampliseq",
    schedule=None,
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
        # TODO what if we move or want to use it in a different context? a good way to configure this?
        base_path = "/data/MicrobiomeDB/common/amplicon_sequencing/"

        # first see whats been run before and under what conditions
        # TODO its possible this info is better in a postgres table
        # though its nice to be able to remove a row to force a rerun
        # and thats easier for most people if its in a csv
        provenance_path = f"{base_path}/processed_studies_provenance.csv"
        with open(provenance_path, 'r') as file:
            next(file)
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
            next(file)
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

        # update code revision etc in processed_studies_provenance.csv
        with open(provenance_path, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['study', 'timestamp', 'code_revision'])
            for study in studies:
                writer.writerow([study, current_timestamp, ampliseq_version])

        config_path = f"{base_path}/ampliseq.config" # TODO validate exists
        
        nextflow_commands = []
        for i in range(len(studies)):
            study = studies[i]
            study_in_path = paths[i]
            study_samplesheet_path = f"{study_in_path}/samplesheet.csv" # TODO validate exists
            study_params_path = f"{study_in_path}/nf-params.json" # TODO validate exists
            study_out_path = f"{study_in_path}/out"

            nextflow_command = (f"nextflow run nf-core/ampliseq -with-trace "
                                f"-r {ampliseq_version} "
                                f"-c {config_path} "
                                f"--params {study_params_path} "
                                f"--input {study_samplesheet_path} "
                                f"--outdir {study_out_path}"
                                f" --profile docker")
            
            nextflow_commands.append(nextflow_command)

        return(nextflow_commands)
        
    @task
    def run_ampliseq(nextflow_command):
        BashOperator(
            task_id="run_ampliseq",
            bash_command=nextflow_command
        )

    nextflow_commands = process_ampliseq_studies()
    run_ampliseq.expand(nextflow_commands)

if __name__ == "__main__":
    dag.test()

