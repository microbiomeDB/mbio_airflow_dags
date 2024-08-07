import pendulum
import csv
import os

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="automated_ampliseq",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
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
                    if ((current_timestamp > float(processed_studies[study]['timestamp'])) or
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

        config_path = f"{base_path}ampliseq.config" # TODO validate exists
        
        commands = []
        for i in range(len(studies)):
            study = studies[i]
            study_in_path = paths[i]
            study_samplesheet_path = f"{study_in_path}/samplesheet.csv" # TODO validate exists
            study_params_path = f"{study_in_path}/nf-params.json" # TODO validate exists
            study_out_path = f"{study_in_path}/out"
            study_work_dir = f"{study_in_path}/work"

            nextflow_command = (f"nextflow run nf-core/ampliseq -with-trace "
                                f"-r {ampliseq_version} "
                                f"-c {config_path} "
                                f"-params-file {study_params_path} "
                                f"-work-dir {study_work_dir} "
                                f"--input {study_samplesheet_path} "
                                f"--outdir {study_out_path} "
                                f"-profile docker")
            
            # TODO dont want a hardcoded path here
            R_command = (f"Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/ampliseq_postProcessing.R {study} {study_out_path}")

            command = (nextflow_command + "; " + R_command)

            commands.append(command)

        return(commands)

    commands = process_ampliseq_studies()
    BashOperator.partial(task_id="run_ampliseq", do_xcom_push=False).expand(
        bash_command=commands
    )

if __name__ == "__main__":
    dag.test()

