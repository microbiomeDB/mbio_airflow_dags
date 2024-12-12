from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import tarfile
import os

# Global Variables
# Change out variables in <> prior to run
USERNAME = '<USER NAME>'
KEY_FILE = '<PATH TO YOUR KEY FILE>'
REMOTE_HOST_SFTP = 'mercury.pmacs.upenn.edu'
REMOTE_HOST_SSH = 'consign.pmacs.upenn.edu'
LOCAL_PATH = '<LOCAL DATA PATH>'
REMOTE_PATH = '<REMOTE DATA PATH>'
RESULTS_FOLDER = 'output'
POKE_INTERVAL = 300  # Interval in seconds for checking job status

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
        with self.ssh_hook.get_conn() as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command('bjobs')
            output = stdout.read().decode()
            if "JOBID" in output:
                self.log.info("Jobs still running. Waiting...")
                return False
            else:
                self.log.info("No unfinished job found. Task complete.")
                return True

# Define DAG arguments
default_args = {
    'owner': USERNAME,
    'start_date': datetime(2024, 1, 1),
    'retries': 10,
    'retry_delay': timedelta(seconds=2),
}

# Define the DAG
dag = DAG(
    'cluster_ssh_sftp_operator_test',
    default_args=default_args,
    description='Test DAG using SSH and SFTP operators to transfer files and execute submission scripts',
    schedule_interval=None,
)

# Task 1: Generate local submission script for remote LSF cluster
def create_shell_script(**context):
    # Get the DAG name
    dag_name = context['dag'].dag_id

    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create unique script name and job name
    script_name = f"{dag_name}_{timestamp}.sh"
    job_name = f"{dag_name}_{timestamp}"

    # Define the script content with the unique job name
    script_content = f'''#!/bin/bash
#BSUB -J {job_name}
mkdir -p {REMOTE_PATH}/{RESULTS_FOLDER}
for i in {{1..999999}}
    do
        let j=1000000-i
        echo "1000000-$i=$j" >> {REMOTE_PATH}/{RESULTS_FOLDER}/{job_name}.txt
    done

# Compress the results folder into a tar.gz file
tar -czf {REMOTE_PATH}/{job_name}_results.tar.gz -C {REMOTE_PATH} {RESULTS_FOLDER}

exit
    '''

    # Path to save the script locally
    script_path = os.path.join(LOCAL_PATH, script_name)

    # Create the local results folder if it doesn't exist
    local_results_path = os.path.join(LOCAL_PATH, RESULTS_FOLDER)
    if not os.path.exists(local_results_path):
        os.makedirs(local_results_path)

    # Write the script content to the file
    with open(script_path, 'w') as file:
        file.write(script_content)

    # Push script name and compressed file name to XCom
    context['ti'].xcom_push(key='script_name', value=script_name)
    context['ti'].xcom_push(key='compressed_file', value=f"{job_name}_results.tar.gz")

setup_environment_task = PythonOperator(
    task_id='setup_environment_and_script',
    python_callable=create_shell_script,
    provide_context=True,
    dag=dag,
)

# Task 2: Compress the entire local path directory into a tar.gz file
def compress_local_directory(**context):
    tar_filename = "local_data.tar.gz"
    local_tar_path = os.path.join(LOCAL_PATH, tar_filename)

    # Create a tar.gz file of the entire LOCAL_PATH
    with tarfile.open(local_tar_path, "w:gz") as tar:
        tar.add(LOCAL_PATH, arcname=os.path.basename(LOCAL_PATH))

    # Push the tar filename to XCom
    context['ti'].xcom_push(key='local_tar_file', value=tar_filename)

compress_directory_task = PythonOperator(
    task_id='compress_local_directory',
    python_callable=compress_local_directory,
    provide_context=True,
    dag=dag,
)

# Task 3: Transfer the tar.gz file to the remote server
def transfer_compressed_directory(**context):
    tar_filename = context['ti'].xcom_pull(key='local_tar_file', task_ids='compress_local_directory')
    local_tar_path = os.path.join(LOCAL_PATH, tar_filename)
    remote_tar_path = os.path.join(REMOTE_PATH, tar_filename)

    # Use SFTPOperator to transfer the tar.gz file
    return SFTPOperator(
        task_id='transfer_compressed_directory',
        ssh_hook=SSHHook(
            remote_host=REMOTE_HOST_SFTP,
            username=USERNAME,
            key_file=KEY_FILE
        ),
        local_filepath=local_tar_path,
        remote_filepath=remote_tar_path,
        operation='put',
        create_intermediate_dirs=True,
        dag=dag,
    ).execute(context)

transfer_compressed_directory_task = PythonOperator(
    task_id='transfer_compressed_directory_task',
    python_callable=transfer_compressed_directory,
    provide_context=True,
    dag=dag,
)

# Task 4: Extract the tar.gz file on the remote server
def extract_remote_directory(**context):
    tar_filename = context['ti'].xcom_pull(key='local_tar_file', task_ids='compress_local_directory')
    remote_tar_path = os.path.join(REMOTE_PATH, tar_filename)
    # Extract without creating a directory layer
    command = f'mkdir -p {REMOTE_PATH} && tar --strip-components=1 -xzf {remote_tar_path} -C {REMOTE_PATH} && rm {remote_tar_path}'

    # Use SSHOperator to extract the tar.gz file and remove it
    return CustomSSHOperator(
        task_id='extract_remote_directory',
        ssh_hook=SSHHook(
            remote_host=REMOTE_HOST_SSH,
            username=USERNAME,
            key_file=KEY_FILE
        ),
        command=command,
        dag=dag,
    ).execute(context)

extract_remote_directory_task = PythonOperator(
    task_id='extract_remote_directory_task',
    python_callable=extract_remote_directory,
    provide_context=True,
    dag=dag,
)

# Task 5: Submit the script via SSH Operator
def submit_script(**context):
    script_name = context['ti'].xcom_pull(key='script_name', task_ids='setup_environment_and_script')
    command = f'bsub -n 64 -M 250000 -R "rusage[mem=250000] span[hosts=1]" sh {REMOTE_PATH}/{script_name}'

    # Use CustomSSHOperator to submit the script
    return CustomSSHOperator(
        task_id='submit_script',
        ssh_hook=SSHHook(
            remote_host=REMOTE_HOST_SSH,
            username=USERNAME,
            key_file=KEY_FILE
        ),
        command=command,
        dag=dag,
    ).execute(context)

submit_script_task = PythonOperator(
    task_id='submit_script_task',
    python_callable=submit_script,
    provide_context=True,
    dag=dag,
)

# Task 6: Monitor the job status
monitor_status_task = JobStatusSensor(
    task_id='monitor_status',
    remote_host=REMOTE_HOST_SSH,
    username=USERNAME,
    key_file=KEY_FILE,
    poke_interval=POKE_INTERVAL,
    dag=dag,
)

# Task 7: Transfer the compressed results file back to local
def transfer_compressed_results(**context):
    compressed_file = context['ti'].xcom_pull(key='compressed_file', task_ids='setup_environment_and_script')
    remote_compressed_path = os.path.join(REMOTE_PATH, compressed_file)
    local_compressed_path = os.path.join(LOCAL_PATH, compressed_file)

    # Use SFTPOperator to transfer the compressed file
    return SFTPOperator(
        task_id='transfer_compressed_results',
        ssh_hook=SSHHook(
            remote_host=REMOTE_HOST_SFTP,
            username=USERNAME,
            key_file=KEY_FILE
        ),
        local_filepath=local_compressed_path,
        remote_filepath=remote_compressed_path,
        operation='get',
        create_intermediate_dirs=True,
        dag=dag,
    ).execute(context)

transfer_compressed_results_task = PythonOperator(
    task_id='transfer_compressed_results_task',
    python_callable=transfer_compressed_results,
    provide_context=True,
    dag=dag,
)

# Task 8: Extract the compressed results file locally and remove tar.gz file
def extract_results_file(**context):
    compressed_file = context['ti'].xcom_pull(key='compressed_file', task_ids='setup_environment_and_script')
    local_compressed_path = os.path.join(LOCAL_PATH, compressed_file)
    local_extract_path = LOCAL_PATH

    # Extract the tar.gz file
    with tarfile.open(local_compressed_path, 'r:gz') as tar:
        tar.extractall(path=local_extract_path)

    # Remove the tar.gz file after extraction
    os.remove(local_compressed_path)

extract_results_task = PythonOperator(
    task_id='extract_results_file',
    python_callable=extract_results_file,
    provide_context=True,
    dag=dag,
)

# Task 9: Clean up local tar files
def clean_up_local_files(**context):
    tar_filename = context['ti'].xcom_pull(key='local_tar_file', task_ids='compress_local_directory')
    local_tar_path = os.path.join(LOCAL_PATH, tar_filename)
    compressed_file = context['ti'].xcom_pull(key='compressed_file', task_ids='setup_environment_and_script')
    local_compressed_path = os.path.join(LOCAL_PATH, compressed_file)

    # Remove the local tar files
    if os.path.exists(local_tar_path):
        os.remove(local_tar_path)
    if os.path.exists(local_compressed_path):
        os.remove(local_compressed_path)

clean_up_local_files_task = PythonOperator(
    task_id='clean_up_local_files',
    python_callable=clean_up_local_files,
    provide_context=True,
    dag=dag,
)

# Task 10: Clean up remote directory contents
def clean_remote_directory(**context):
    # Command to remove all contents in the REMOTE_PATH
    command = f'rm -rf {REMOTE_PATH}/*'

    # Use SSHOperator to clean up the remote directory
    return CustomSSHOperator(
        task_id='clean_remote_directory',
        ssh_hook=SSHHook(
            remote_host=REMOTE_HOST_SSH,
            username=USERNAME,
            key_file=KEY_FILE
        ),
        command=command,
        dag=dag,
    ).execute(context)

clean_remote_directory_task = PythonOperator(
    task_id='clean_remote_directory_task',
    python_callable=clean_remote_directory,
    provide_context=True,
    dag=dag,
)


# Setting up the task dependencies
setup_environment_task >> compress_directory_task >> transfer_compressed_directory_task >> extract_remote_directory_task >> submit_script_task >> monitor_status_task >> transfer_compressed_results_task >> extract_results_task >> clean_up_local_files_task >> clean_remote_directory_task
