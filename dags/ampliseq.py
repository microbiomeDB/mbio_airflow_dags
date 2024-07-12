from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from os.path import join
import pendulum
import csv
import os
import textwrap

# Constants for file paths
BASE_PATH = "/home/ruicatxiao/mbio_af_branch/local_testing_data_config"
PROVENANCE_PATH = join(BASE_PATH, "processed_studies_provenance.csv")
ALL_STUDIES_PATH = join(BASE_PATH, "amplicon_studies.csv")
CONFIG_PATH = join(BASE_PATH, "ampliseq.config")

def load_processed_studies():
    """Load processed studies from a CSV file into a dictionary."""
    processed_studies = {}
    if os.path.exists(PROVENANCE_PATH):
        with open(PROVENANCE_PATH, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 'code_revision': row['code_revision']}
    return processed_studies

def update_provenance_file(study, timestamp, code_revision='2.9.0'):
    """Update the provenance file with the latest processing information."""
    fieldnames = ['study', 'timestamp', 'code_revision']
    new_data = {'study': study, 'timestamp': timestamp, 'code_revision': code_revision}
    with open(PROVENANCE_PATH, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.stat(PROVENANCE_PATH).st_size == 0:
            writer.writeheader()
        writer.writerow(new_data)

def create_dag():
    default_args = {
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
    }

    with DAG(
        dag_id="rx_test_automated_ampliseq_v5",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:
        processed_studies = load_processed_studies()

        with TaskGroup("nextflow_tasks", tooltip="Nextflow processing tasks") as nextflow_tasks, \
             TaskGroup("rscript_tasks", tooltip="R script processing tasks") as rscript_tasks:

            if os.path.exists(ALL_STUDIES_PATH):
                with open(ALL_STUDIES_PATH, "r") as file:
                    next(file)  # Skip header
                    for line in file:
                        study, path = line.strip().split(",")
                        current_timestamp = os.path.getmtime(path)
                        process_study = study not in processed_studies or processed_studies[study]['timestamp'] != str(current_timestamp)

                        if process_study:
                            templated_command_nextflow = textwrap.dedent("""
                                nextflow run nf-core/ampliseq -with-tower -r 2.9.0 \
                                -params-file {{ params.study_params_path }} \
                                -work-dir {{ params.study_work_dir }} --input {{ params.study_samplesheet_path }} \
                                --outdir {{ params.study_out_path }} \
                                -c {{ params.study_config_path }} \
                                -profile docker
                            """)

                            templated_command_rscript = textwrap.dedent("""
                                Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/ampliseq_postProcessing.R \
                                {{ params.study }} {{ params.study_out_path }}
                            """)

                            nextflow_task = BashOperator(
                                task_id=f'nextflow_{study}',
                                bash_command=templated_command_nextflow,
                                params={
                                    'study_params_path': join(path, "nf-params.json"),
                                    'study_work_dir': join(path, "work"),
                                    'study_samplesheet_path': join(path, "samplesheet.csv"),
                                    'study_out_path': join(path, "out"),
                                    'study_config_path': CONFIG_PATH
                                },
                                task_group=nextflow_tasks
                            )

                            rscript_task = BashOperator(
                                task_id=f'rscript_{study}',
                                bash_command=templated_command_rscript,
                                params={
                                    'study': study,
                                    'study_out_path': f"{path}/out"
                                },
                                task_group=rscript_tasks
                            )

                            # Create a task to update the provenance file after processing
                            update_provenance_task = PythonOperator(
                                task_id='update_provenance_{}'.format(study),
                                python_callable=update_provenance_file,
                                op_kwargs={'study': study, 'timestamp': str(current_timestamp)},
                                dag=dag
                            )

                            nextflow_task >> rscript_task >> update_provenance_task
            else:
                raise FileNotFoundError(f"Studies file not found: {ALL_STUDIES_PATH}")

        return dag

# Define the DAG by calling the function
ampliseq_pipeline = create_dag()
