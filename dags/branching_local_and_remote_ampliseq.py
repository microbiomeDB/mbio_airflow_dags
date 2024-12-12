import os, csv, logging, tarfile, pendulum, textwrap
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule

# Constants for file paths and configurations
# Currently does not work, WIP!!!
# Currently does not work, WIP!!!

BASE_PATH = "<path to local data directory>"
PROVENANCE_PATH = os.path.join(BASE_PATH, "processed_studies_provenance.csv")
ALL_STUDIES_PATH = os.path.join(BASE_PATH, "amplicon_studies.csv")
CONFIG_PATH = os.path.join(BASE_PATH, "ampliseq.config")
AMPLISEQ_VERSION = '2.9.0'
USERNAME = 'ruicatx'
KEY_FILE = '<path to ssh key file>'
REMOTE_HOST_SFTP = 'mercury.pmacs.upenn.edu'
REMOTE_HOST_SSH = 'consign.pmacs.upenn.edu'
REMOTE_PATH = '<path to remote data directory>'
POKE_INTERVAL = 300  # Interval in seconds for checking job status

# Default arguments for the DAG
default_args = {
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    "retries": 10,
    'retry_delay': timedelta(seconds=2),
}

class CustomSSHOperator(SSHOperator):
    template_fields = ()  # Override to prevent command field from being templated

class JobStatusSensor(BaseSensorOperator):
    def __init__(self, remote_host, username, key_file, *args, **kwargs):
        super(JobStatusSensor, self).__init__(*args, **kwargs)
        self.remote_host = remote_host
        self.username = username
        self.key_file = key_file
        self.ssh_hook = SSHHook(remote_host=self.remote_host, username=self.username, key_file=self.key_file)

    def poke(self, context):
        try:
            with self.ssh_hook.get_conn() as ssh_client:
                stdin, stdout, stderr = ssh_client.exec_command('bjobs')
                output = stdout.read().decode()
                if "JOBID" in output:
                    self.log.info("Jobs still running. Waiting...")
                    return False
                else:
                    self.log.info("No unfinished job found. Task complete.")
                    return True
        except Exception as e:
            logging.error(f"Error checking job status: {e}")
            return False


def choose_execution_path(run_locations):
    # Check if the list is empty
    if not run_locations:
        logging.info("No studies to process, skipping all downstream tasks.")
        return []

    # Flatten the list in case it's nested
    if isinstance(run_locations[0], list):
        run_locations = [item for sublist in run_locations for item in sublist]

    logging.info(f"Flattened run locations: {run_locations}")

    task_ids = []
    for run_location in run_locations:
        if run_location == 'local':
            task_ids.append('process_amplicon_studies.local_ampliseq.run_ampliseq')
        elif run_location == 'remote':
            task_ids.append('process_amplicon_studies.remote_ampliseq.compress_local_directory')
        else:
            raise ValueError(f"Invalid run location: {run_location}")

    if task_ids:
        logging.info(f"Selected task IDs for execution: {task_ids}")
    else:
        logging.info("No valid task IDs selected, all downstream tasks will be skipped.")

    return task_ids


def create_dag():
    with DAG(
        dag_id="automated_local_and_remote_ampliseq_v2",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:


        @dag.task()
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
                                study_info = {
                                    'study': study,
                                    'path': study_path,
                                    'current_timestamp': current_timestamp,
                                    'run_location': run_location
                                }
                                studies.append(study_info)
                                logging.info(f"Loaded study: {study}, Path: {study_path}, Run Location: {run_location}")

            if not studies:
                logging.info("No studies to process.")

            return studies
        

        @dag.task()
        def extract_run_location(studies):
            return [study['run_location'] for study in studies]


        @dag.task(trigger_rule=TriggerRule.NONE_FAILED)
        def update_provenance(studies):
            # Implementation of update_provenance
            fieldnames = ['study', 'timestamp', 'code_revision']
            updated_studies = []

            existing_data = []
            if os.path.exists(PROVENANCE_PATH):
                with open(PROVENANCE_PATH, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_data = list(reader)

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

                all_data = {d['study']: d for d in existing_data}
                all_data.update({d['study']: d for d in updated_studies})

                writer.writerows(all_data.values())
                logging.info(f"Updated provenance for studies: {', '.join([s['study'] for s in studies])}")





        def create_shell_script(study):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            script_name = f"{study['study']}_{timestamp}.sh"
            job_name = f"{study['study']}_{timestamp}"
            script_content = f'''#!/bin/bash
#BSUB -J {job_name}
mkdir -p {REMOTE_PATH}/{study['study']}/out
mkdir -p {REMOTE_PATH}/{study['study']}/work
nextflow run nf-core/ampliseq -with-tower -r {AMPLISEQ_VERSION} \
-params-file {REMOTE_PATH}/{study['study']}/nf-params.json \
-work-dir {REMOTE_PATH}/{study['study']}/work --input {REMOTE_PATH}/{study['study']}/samplesheet.csv \
--outdir {REMOTE_PATH}/{study['study']}/out \
-c {REMOTE_PATH}/ampliseq.config \
-profile singularity
module load R/4.0.2
Rscript {REMOTE_PATH}/ampliseq_postProcessing.R \
{study['study']} {REMOTE_PATH}/{study['study']}/out
module unload R/4.0.2
exit
            '''
            script_path = os.path.join(study['path'], script_name)
            with open(script_path, 'w') as file:
                file.write(script_content)
            return script_name

        def compress_local_directory(study):
            tar_filename = f"{study['study']}_data.tar.gz"
            local_tar_path = os.path.join(study['path'], tar_filename)
            with tarfile.open(local_tar_path, "w:gz") as tar:
                tar.add(study['path'], arcname=os.path.basename(study['path']))
            return tar_filename

        def transfer_compressed_directory(study, tar_filename):
            local_tar_path = os.path.join(study['path'], tar_filename)
            remote_tar_path = os.path.join(REMOTE_PATH, study['study'], tar_filename)
            sftp_op = SFTPOperator(
                task_id='transfer_compressed_directory_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SFTP,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                local_filepath=local_tar_path,
                remote_filepath=remote_tar_path,
                operation='put',
                create_intermediate_dirs=True
            )
            sftp_op.execute(context={})

        def extract_remote_directory(study, tar_filename):
            remote_tar_path = os.path.join(REMOTE_PATH, study['study'], tar_filename)
            command = f'mkdir -p {REMOTE_PATH}/{study["study"]} && tar --strip-components=1 -xzf {remote_tar_path} -C {REMOTE_PATH}/{study["study"]} && rm {remote_tar_path}'
            ssh_op = CustomSSHOperator(
                task_id='extract_remote_directory_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        def submit_script(study, script_name):
            command = f'bsub -n 64 -M 250000 -R "rusage[mem=250000] span[hosts=1]" sh {REMOTE_PATH}/{study["study"]}/{script_name}'
            ssh_op = CustomSSHOperator(
                task_id='submit_script_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        def clean_up_remote(study):
            command = f'rm {REMOTE_PATH}/{study["study"]}/samplesheet.csv {REMOTE_PATH}/{study["study"]}/nf-params.json {REMOTE_PATH}/{study["study"]}/{study["study"]}*.sh && tar -czf {REMOTE_PATH}/{study["study"]}_results.tar.gz -C {REMOTE_PATH}/{study["study"]} out work && rm -rf {REMOTE_PATH}/{study["study"]}/data'
            ssh_op = CustomSSHOperator(
                task_id='clean_up_remote_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        def transfer_compressed_results(study):
            remote_compressed_path = os.path.join(REMOTE_PATH, f"{study['study']}_results.tar.gz")
            local_compressed_path = os.path.join(study['path'], f"{study['study']}_results.tar.gz")
            sftp_op = SFTPOperator(
                task_id='transfer_compressed_results_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SFTP,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                local_filepath=local_compressed_path,
                remote_filepath=remote_compressed_path,
                operation='get',
                create_intermediate_dirs=True
            )
            sftp_op.execute(context={})

        def extract_results_file(study):
            local_compressed_path = os.path.join(study['path'], f"{study['study']}_results.tar.gz")
            local_extract_path = study['path']
            with tarfile.open(local_compressed_path, 'r:gz') as tar:
                tar.extractall(path=local_extract_path)
            os.remove(local_compressed_path)



        loaded_studies = load_studies()

        run_locations = extract_run_location(loaded_studies)


        with TaskGroup("process_amplicon_studies") as process_amplicon_studies:
            branching = BranchPythonOperator(
                task_id='branching',
                python_callable=choose_execution_path,
                op_args=[run_locations],
            )

            with TaskGroup("remote_ampliseq") as remote_ampliseq:

                # Expand the process to each study returned by loaded_studies
                script_name_task = dag.task(create_shell_script).expand(study=loaded_studies)

                compress_task = BashOperator(
                    task_id="compress_local_directory",
                    bash_command="tar -czf {{ params.study.study }}_data.tar.gz -C {{ params.study.path }} .",
                    params={"study": "{{ ti.xcom_pull(task_ids='script_name_task') }}"},
                    dag=dag,
                )

                transfer_task = SFTPOperator(
                    task_id="transfer_compressed_directory",
                    ssh_hook=SSHHook(
                        remote_host=REMOTE_HOST_SFTP,
                        username=USERNAME,
                        key_file=KEY_FILE,
                    ),
                    local_filepath="{{ ti.xcom_pull(task_ids='script_name_task')['path'] }}/{{ ti.xcom_pull(task_ids='script_name_task')['study'] }}_data.tar.gz",
                    remote_filepath="{{ ti.xcom_pull(task_ids='script_name_task')['remote_path'] }}/{{ ti.xcom_pull(task_ids='script_name_task')['study'] }}_data.tar.gz",
                    operation="put",
                    create_intermediate_dirs=True,
                    dag=dag,
                )

                extract_task = CustomSSHOperator(
                    task_id="extract_remote_directory",
                    ssh_hook=SSHHook(
                        remote_host=REMOTE_HOST_SSH,
                        username=USERNAME,
                        key_file=KEY_FILE,
                    ),
                    command="mkdir -p {{ params.study.remote_path }}/{{ params.study.study }} && tar --strip-components=1 -xzf {{ params.study.remote_tar_path }} -C {{ params.study.remote_path }}/{{ params.study.study }}",
                    params={"study": "{{ ti.xcom_pull(task_ids='script_name_task') }}"},
                    dag=dag,
                )


                update_samplesheet_task = CustomSSHOperator(
                        task_id="update_samplesheet_paths",
                        ssh_hook=SSHHook(
                            remote_host=REMOTE_HOST_SSH,
                            username=USERNAME,
                            key_file=KEY_FILE,
                        ),
                        command="sed -i 's|/home/ruicatxiao/mbio_af_branch/local_testing_data_config/|/home/ruicatx/airflow/|g' {{ REMOTE_PATH }}/{{ ti.xcom_pull(task_ids='load_studies')[0]['study'] }}/samplesheet.csv",
                        dag=dag,
                    )

                submit_task = CustomSSHOperator(
                    task_id="submit_script",
                    ssh_hook=SSHHook(
                        remote_host=REMOTE_HOST_SSH,
                        username=USERNAME,
                        key_file=KEY_FILE,
                    ),
                    command="bsub -n 64 -M 250000 -R 'rusage[mem=250000] span[hosts=1]' sh {{ params.study.remote_path }}/{{ params.study.study }}/{{ ti.xcom_pull(task_ids='script_name_task')['script_name'] }}",
                    params={"study": "{{ ti.xcom_pull(task_ids='script_name_task') }}"},
                    dag=dag,
                )

                monitor_task = JobStatusSensor(
                    task_id="monitor_status",
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE,
                    poke_interval=POKE_INTERVAL,
                    dag=dag,
                )

                clean_task = CustomSSHOperator(
                    task_id="clean_up_remote",
                    ssh_hook=SSHHook(
                        remote_host=REMOTE_HOST_SSH,
                        username=USERNAME,
                        key_file=KEY_FILE,
                    ),
                    command="rm {{ params.study.remote_path }}/{{ params.study.study }}/samplesheet.csv {{ params.study.remote_path }}/{{ params.study.study }}/nf-params.json {{ params.study.remote_path }}/{{ params.study.study }}/{{ params.study.study }}*.sh && tar -czf {{ params.study.remote_path }}/{{ params.study.study }}_results.tar.gz -C {{ params.study.remote_path }}/{{ params.study.study }} out work",
                    params={"study": "{{ ti.xcom_pull(task_ids='script_name_task') }}"},
                    dag=dag,
                )

                transfer_back_task = SFTPOperator(
                    task_id="transfer_compressed_results",
                    ssh_hook=SSHHook(
                        remote_host=REMOTE_HOST_SFTP,
                        username=USERNAME,
                        key_file=KEY_FILE,
                    ),
                    local_filepath="{{ ti.xcom_pull(task_ids='script_name_task')['path'] }}/{{ ti.xcom_pull(task_ids='script_name_task')['study'] }}_results.tar.gz",
                    remote_filepath="{{ ti.xcom_pull(task_ids='script_name_task')['remote_path'] }}/{{ ti.xcom_pull(task_ids='script_name_task')['study'] }}_results.tar.gz",
                    operation="get",
                    create_intermediate_dirs=True,
                    dag=dag,
                )

                extract_results_task = BashOperator(
                    task_id="extract_results_file",
                    bash_command="tar -xzf {{ ti.xcom_pull(task_ids='script_name_task')['path'] }}/{{ ti.xcom_pull(task_ids='script_name_task')['study'] }}_results.tar.gz -C {{ ti.xcom_pull(task_ids='script_name_task')['path'] }}",
                    dag=dag,
                )

                compress_task >> transfer_task >> extract_task >> update_samplesheet_task >> submit_task >> monitor_task >> clean_task >> transfer_back_task >> extract_results_task


            with TaskGroup("local_ampliseq") as local_ampliseq:
                run_ampliseq = BashOperator(
                    task_id='run_ampliseq',
                    bash_command=textwrap.dedent("""
                        nextflow run nf-core/ampliseq -with-tower -r {{ params.ampliseq_version }} \
                        -params-file "{{ ti.xcom_pull(task_ids='load_studies')[0]['path'] }}/nf-params.json" \
                        -work-dir "{{ ti.xcom_pull(task_ids='load_studies')[0]['path'] }}/work" \
                        --input "{{ ti.xcom_pull(task_ids='load_studies')[0]['path'] }}/samplesheet.csv" \
                        --outdir "{{ ti.xcom_pull(task_ids='load_studies')[0]['path'] }}/out" \
                        -c {{ params.study_config_path }} \
                        -profile docker
                    """),
                    params={
                        'ampliseq_version': AMPLISEQ_VERSION,
                        'study_config_path': CONFIG_PATH,
                    },
                    dag=dag,
                )

                run_r_postprocessing = BashOperator(
                    task_id='run_r_postprocessing',
                    bash_command=textwrap.dedent("""
                        Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/ampliseq_postProcessing.R \
                        "{{ ti.xcom_pull(task_ids='load_studies')[0]['study'] }}" \
                        "{{ ti.xcom_pull(task_ids='load_studies')[0]['path'] }}/out"
                    """),
                    dag=dag,
                )

                run_ampliseq >> run_r_postprocessing

            branching >> [local_ampliseq, remote_ampliseq]

        update_provenance_task = update_provenance(loaded_studies)
        [local_ampliseq, remote_ampliseq] >> update_provenance_task
#        process_amplicon_studies >> update_provenance_task

    return dag


ampliseq_pipeline = create_dag()
