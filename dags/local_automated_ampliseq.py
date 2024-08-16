from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import pendulum
import csv
import os
import textwrap
from datetime import timedelta

# Constants for file paths
BASE_PATH = "<Path to directory with data>"
PROVENANCE_PATH = os.path.join(BASE_PATH, "processed_studies_provenance.csv")
ALL_STUDIES_PATH = os.path.join(BASE_PATH, "amplicon_studies.csv")
CONFIG_PATH = os.path.join(BASE_PATH, "ampliseq.config")
AMPLISEQ_VERSION = '2.9.0'

def create_dag():
    default_args = {
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
    }

    with DAG(
        dag_id="rx_test_local_ampliseq_v1",
        schedule_interval=None,
        default_args={
            "retries": 10,
            'retry_delay': timedelta(seconds=2),
        },
        catchup=False,
    ) as dag:

        @task
        def load_studies():
            processed_studies_dict = {}
            if os.path.exists(PROVENANCE_PATH):
                with open(PROVENANCE_PATH, 'r') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        processed_studies_dict[row['study']] = {
                            'timestamp': row['timestamp'],
                            'code_revision': row['code_revision']
                        }

            studies = []
            if os.path.exists(ALL_STUDIES_PATH):
                with open(ALL_STUDIES_PATH, "r") as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        study = row['study']
                        study_path = row['studyPath']
                        run_location = row['RunLocation']

                        # Skip studies with RunLocation set to 'remote'
                        if run_location == 'remote':
                            continue
                        
                        samplesheet_path = os.path.join(study_path, "samplesheet.csv")
                        if os.path.exists(samplesheet_path):
                            current_timestamp = str(os.path.getmtime(samplesheet_path))
                            
                            process_study = False
                            if study not in processed_studies_dict:
                                process_study = True
                            else:
                                stored_data = processed_studies_dict[study]
                                if (current_timestamp > stored_data['timestamp'] or
                                    AMPLISEQ_VERSION != stored_data['code_revision']):
                                    process_study = True

                            if process_study:
                                studies.append({
                                    'study': study,
                                    'path': study_path,
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
                        'code_revision': AMPLISEQ_VERSION
                    }
                    updated_studies.append(updated_study)
                
                # Merge with existing data
                all_data = {d['study']: d for d in existing_data}
                all_data.update({d['study']: d for d in updated_studies})
                
                # Write back to the file
                writer.writerows(all_data.values())

        loaded_studies = load_studies()

        with TaskGroup("process_amplicon_studies", tooltip="Process Amplicon Studies") as process_amplicon_studies:
            run_ampliseq = BashOperator.partial(
                task_id='run_ampliseq',
                bash_command=textwrap.dedent("""\
                    nextflow run nf-core/ampliseq -with-tower -r {{ params.ampliseq_version }} \
                    -params-file {{ params.study_params_path }} \
                    -work-dir {{ params.study_work_dir }} --input {{ params.study_samplesheet_path }} \
                    --outdir {{ params.study_out_path }} \
                    -c {{ params.study_config_path }} \
                    -profile docker
                """)
            ).expand(
                params=loaded_studies.map(lambda x: {
                    'ampliseq_version': AMPLISEQ_VERSION,
                    'study_params_path': os.path.join(x['path'], "nf-params.json"),
                    'study_work_dir': os.path.join(x['path'], "work"),
                    'study_samplesheet_path': os.path.join(x['path'], "samplesheet.csv"),
                    'study_out_path': os.path.join(x['path'], "out"),
                    'study_config_path': CONFIG_PATH
                })
            ) >> BashOperator.partial(  # Chained
                task_id='run_r_postprocessing',
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

        run_ampliseq >> update_provenance_task  

        return dag


ampliseq_pipeline = create_dag()
