from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import pendulum
import csv
import os
import textwrap

# Constants for file paths
# Modify the base path and make sure all relevent files exist in path for the DAG to pull on
# processed_studies_provenance.csv is used for logging samples processed by DAG already
# ampliseq.config is used to feed seqera cloud monitoring token into nextflow
# amplicon_studies.csv is used for pointing to studies and their path

BASE_PATH = "/home/ruicatxiao/mbio_af_branch/local_testing_data_config"
PROVENANCE_PATH = os.path.join(BASE_PATH, "processed_studies_provenance.csv")
ALL_STUDIES_PATH = os.path.join(BASE_PATH, "amplicon_studies.csv")
CONFIG_PATH = os.path.join(BASE_PATH, "ampliseq.config")

# maybe need to get airflow version instead of hard code it here?
AUOTMATED_AMPLISEQ_VERSION = '2.9.0'

def create_dag():
    default_args = {
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
    }



# now at v6 of auto-amp-dag
# list of fixed tasks
# from manual looping to dynamic task mapping
# jinja tempplating of tasks, leaning heavily on Bash Operators
# Proper tower acces via nextflow config file
# When encountering the bug regarding "cannot read served logs", need to clear failed tasks and it will attempt to re-run, should complete most of the time


# Potentially to do list, have the DAG scheduled to run and check on a daily or weekly basis. Alternatively just keep it on a manual triggering


    with DAG(
        dag_id="rx_test_automated_ampliseq_v6",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:

        @task
        def load_studies():
            processed_studies = {}
            if os.path.exists(PROVENANCE_PATH):
                with open(PROVENANCE_PATH, 'r') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        processed_studies[row['study']] = row['code_revision']

            studies = []
            if os.path.exists(ALL_STUDIES_PATH):
                with open(ALL_STUDIES_PATH, "r") as file:
                    next(file)  # Skip header
                    for line in file:
                        study, path = line.strip().split(",")
                        if (study not in processed_studies or
                            processed_studies[study] != AUOTMATED_AMPLISEQ_VERSION):
                            current_timestamp = str(os.path.getmtime(path))
                            studies.append({
                                'study': study,
                                'path': path,
                                'current_timestamp': current_timestamp
                            })
            return studies

        @task
        def update_provenance(studies):
            fieldnames = ['study', 'timestamp', 'code_revision']
            updated_studies = []

            # Read existing data
            existing_data = []
            if os.path.exists(PROVENANCE_PATH):
                with open(PROVENANCE_PATH, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_data = list(reader)

            # Update or append new data
            with open(PROVENANCE_PATH, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for study in studies:
                    updated_study = {
                        'study': study['study'],
                        'timestamp': study['current_timestamp'],
                        'code_revision': AUOTMATED_AMPLISEQ_VERSION
                    }
                    updated_studies.append(updated_study)
                
                # Merge with existing data
                all_data = {d['study']: d for d in existing_data}
                all_data.update({d['study']: d for d in updated_studies})
                
                # Write back to the file
                writer.writerows(all_data.values())

        loaded_studies = load_studies()

        with TaskGroup("nextflow_tasks", tooltip="Nextflow processing tasks") as nextflow_tasks:
            nextflow_task = BashOperator.partial(
                task_id='nextflow_task',
                bash_command=textwrap.dedent("""\
                    nextflow run nf-core/ampliseq -with-tower -r 2.9.0 \
                    -params-file {{ params.study_params_path }} \
                    -work-dir {{ params.study_work_dir }} --input {{ params.study_samplesheet_path }} \
                    --outdir {{ params.study_out_path }} \
                    -c {{ params.study_config_path }} \
                    -profile docker
                """)
            ).expand(
                params=loaded_studies.map(lambda x: {
                    'study_params_path': os.path.join(x['path'], "nf-params.json"),
                    'study_work_dir': os.path.join(x['path'], "work"),
                    'study_samplesheet_path': os.path.join(x['path'], "samplesheet.csv"),
                    'study_out_path': os.path.join(x['path'], "out"),
                    'study_config_path': CONFIG_PATH
                })
            )

        with TaskGroup("rscript_tasks", tooltip="R script processing tasks") as rscript_tasks:
            rscript_task = BashOperator.partial(
                task_id='rscript_task',
                bash_command=textwrap.dedent("""\
                    Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/ampliseq_postProcessing.R \
                    {{ params.study }} {{ params.study_out_path }}
                """)
            ).expand(
                params=loaded_studies.map(lambda x: {
                    'study': x['study'],
                    'study_out_path': os.path.join(x['path'], "out")
                })
            )

        update_provenance_task = update_provenance(loaded_studies)

        nextflow_task >> rscript_task >> update_provenance_task

        return dag

# Define the DAG by calling the function
ampliseq_pipeline = create_dag()
