import os, csv, logging, tarfile, pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule

# Constants for file paths and configurations
BASE_PATH = "<PATH TO LOCAL DATA DIRECTORY>"
PROVENANCE_PATH = os.path.join(BASE_PATH, "processed_studies_provenance.csv")
ALL_STUDIES_PATH = os.path.join(BASE_PATH, "amplicon_studies.csv")
CONFIG_PATH = os.path.join(BASE_PATH, "ampliseq.config")
AMPLISEQ_VERSION = '2.9.0'
USERNAME = 'ruicatx'
KEY_FILE = os.path.join(BASE_PATH, "chmi_rsa")
REMOTE_HOST_SFTP = 'mercury.pmacs.upenn.edu'
REMOTE_HOST_SSH = 'consign.pmacs.upenn.edu'
REMOTE_PATH = '<PATH TO REMOTE DATA DIRECTORY>'
REMOTE_CONFIG = '<PATH TO REMOTE CONFIG>'
POKE_INTERVAL = 600  # Interval in seconds for checking job status

# Default arguments for the DAG
default_args = {
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    "retries": 20,
    'retry_delay': timedelta(seconds=10),
}

class CustomSSHOperator(SSHOperator):
    template_fields = ()  # Override to prevent command field from being templated

    def execute(self, context):
        logging.info(f"Executing command: {self.command}")
        return super().execute(context)

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

def create_dag():
    with DAG(
        dag_id="remote_ampliseq",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
    ) as dag:

        @dag.task()
        def load_studies():
            logging.info("Loading studies...")
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

                        if run_location == "local":
                            logging.info(f"Skipping study {study} as it has a local RunLocation.")
                            continue

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
        
        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def update_provenance(studies):
            logging.info("Updating provenance...")
            if not studies:
                logging.info("No studies to update in provenance.")
                return
            
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

        @dag.task()
        def create_shell_script_nextflow(study):
            logging.info(f"Creating Nextflow shell script for study {study['study']}...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            job_name = f"{study['study']}_{timestamp}_nextflow"
            script_name = f"{study['study']}_nextflow_{timestamp}.sh"
            script_content = f'''#!/bin/bash
#BSUB -J {job_name}
mkdir -p {REMOTE_PATH}/{study['study']}/out
mkdir -p {REMOTE_PATH}/{study['study']}/work
nextflow run nf-core/ampliseq -with-tower -r {AMPLISEQ_VERSION} \
-params-file {REMOTE_PATH}/{study['study']}/nf-params.json \
-work-dir {REMOTE_PATH}/{study['study']}/work --input {REMOTE_PATH}/{study['study']}/samplesheet.csv \
--outdir {REMOTE_PATH}/{study['study']}/out \
-c {REMOTE_CONFIG}/ampliseq.config \
-profile singularity
echo "nextflow_done"
exit
    '''
            script_path = os.path.join(study['path'], script_name)
            with open(script_path, 'w') as file:
                file.write(script_content)
            return {"study": study, "script_name": script_name}

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def compress_local_directory(study):
            logging.info(f"Compressing local directory for study {study['study']}...")
            tar_filename = f"{study['study']}_data.tar.gz"
            local_tar_path = os.path.join(study['path'], tar_filename)
            with tarfile.open(local_tar_path, "w:gz") as tar:
                tar.add(study['path'], arcname=os.path.basename(study['path']))
            return {"study": study, "tar_filename": tar_filename}

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def transfer_compressed_directory(tar_info):
            logging.info(f"Transferring compressed directory for study {tar_info['study']['study']}...")
            study = tar_info["study"]
            tar_filename = tar_info["tar_filename"]
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

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def extract_remote_directory(tar_info):
            logging.info(f"Extracting remote directory for study {tar_info['study']['study']}...")
            study = tar_info["study"]
            tar_filename = tar_info["tar_filename"]
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

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def editing_remote_samplesheet(study):
            logging.info(f"Editing remote samplesheet for study {study['study']}...")
            command = f"sed -i 's|{BASE_PATH}/|{REMOTE_PATH}/|g' {REMOTE_PATH}/{study['study']}/samplesheet.csv"
            ssh_op = CustomSSHOperator(
                task_id='editing_remote_samplesheet_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def submit_nextflow_script(script_info):
            logging.info(f"Submitting Nextflow script for study {script_info['study']['study']}...")
            study = script_info["study"]
            script_name = script_info["script_name"]
            command = f'bsub -n 64 -M 250000 -R "rusage[mem=250000] span[hosts=1]" sh {REMOTE_PATH}/{study["study"]}/{script_name}'
            return CustomSSHOperator(
                task_id='submit_nextflow_script_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command,
            ).execute(context={})

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def monitor_status_nextflow(study):
            logging.info(f"Monitoring Nextflow job status for study {study['study']}...")
            job_sensor = JobStatusSensor(
                task_id=f"monitor_status_nextflow_{study['study']}",
                remote_host=REMOTE_HOST_SSH,
                username=USERNAME,
                key_file=KEY_FILE,
                poke_interval=POKE_INTERVAL,
            )
            return job_sensor.execute(context={})

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def compress_remote_results(study):
            logging.info(f"Compressing remote results for study {study['study']}...")
            command = f'tar -czf {REMOTE_PATH}/{study["study"]}_results.tar.gz -C {REMOTE_PATH}/{study["study"]} out'
            ssh_op = CustomSSHOperator(
                task_id='compress_remote_results_' + study['study'],
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def transfer_compressed_results(study):
            logging.info(f"Transferring compressed results for study {study['study']}...")
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

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def extract_results_file(study):
            logging.info(f"Extracting results file for study {study['study']}...")
            local_compressed_path = os.path.join(study['path'], f"{study['study']}_results.tar.gz")
            local_extract_path = study['path']
            with tarfile.open(local_compressed_path, 'r:gz') as tar:
                tar.extractall(path=local_extract_path)
            os.remove(local_compressed_path)

        @dag.task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def local_Rscript_postprocessing(study):
            logging.info(f"Running local Rscript postprocessing for study {study['study']}...")
            command = f'Rscript {BASE_PATH}/ampliseq_postProcessing.R {study["study"]} {study["path"]}/out'
            os.system(command)
            logging.info(f"Postprocessing done for study {study['study']}.")

        @dag.task()
        def clean_up_remote():
            logging.info("Cleaning up remote directory...")
            command = f'rm -rf {REMOTE_PATH}/*'
            ssh_op = CustomSSHOperator(
                task_id='clean_up_remote',
                ssh_hook=SSHHook(
                    remote_host=REMOTE_HOST_SSH,
                    username=USERNAME,
                    key_file=KEY_FILE
                ),
                command=command
            )
            ssh_op.execute(context={})

        @dag.task()
        def clean_up_local():
            clean_up_command = f"find {BASE_PATH}/* -type f \\( -name '*.tar.gz' -o -name '*.sh' \\) -exec rm {{}} +"
            return BashOperator(
                task_id='clean_up_local',
                bash_command=clean_up_command,
            ).execute(context={})

        loaded_studies = load_studies()

        with TaskGroup("process_amplicon_studies") as process_amplicon_studies:
            script_names_nextflow = create_shell_script_nextflow.expand(study=loaded_studies)
            tar_infos = compress_local_directory.expand(study=loaded_studies)
            transfer_compressed_directory = transfer_compressed_directory.expand(tar_info=tar_infos)
            extract_remote_directory = extract_remote_directory.expand(tar_info=tar_infos)
            editing_remote_samplesheet = editing_remote_samplesheet.expand(study=loaded_studies)
            submit_nextflow_script_task = submit_nextflow_script.expand(script_info=script_names_nextflow)
            monitor_status_nextflow = monitor_status_nextflow.expand(study=loaded_studies)
            compress_remote_results = compress_remote_results.expand(study=loaded_studies)
            transfer_compressed_results = transfer_compressed_results.expand(study=loaded_studies)
            extract_results_file = extract_results_file.expand(study=loaded_studies)
            local_Rscript_postprocessing_task = local_Rscript_postprocessing.expand(study=loaded_studies)

            # Adjusted workflow to reflect the correct sequence
            script_names_nextflow >> tar_infos >> transfer_compressed_directory >> extract_remote_directory \
            >> editing_remote_samplesheet >> submit_nextflow_script_task >> monitor_status_nextflow \
            >> compress_remote_results >> transfer_compressed_results >> extract_results_file >> local_Rscript_postprocessing_task

        # Main DAG dependencies
        loaded_studies >> process_amplicon_studies
        process_amplicon_studies >> clean_up_remote() >> clean_up_local() >> update_provenance(loaded_studies)

    return dag

ampliseq_pipeline = create_dag()
