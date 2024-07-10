from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from os.path import join

from mbio_utils.cluster_manager import ClusterManager

import pendulum
import csv
import os
import json

## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
##
##          IMPORTANT NOTE:
##
##          This dag is written as it is because of an Airflow bug: 
##           https://github.com/apache/airflow/issues/39222
##
##          Once this issue is resolved, it should be re-written to use dynamic task mapping
##          over a task group. The task group should have a BranchPythonOperator or similar
##          to allow for fetchngs to be optional. This commit may serve as a reference:
##          https://github.com/microbiomeDB/mbio_airflow_dags/commit/31add62b871706bef8e8deaceaa0e051778e1128
##
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# Constants for file paths
# TODO should some/ all of these be configurable?
CLUSTER_CONFIG_PATH = "/data/MicrobiomeDB/mbio_airflow_dags/cluster_configs/pmacs.json"
BASE_PATH = "/data/MicrobiomeDB/common/shotgun_metagenomics"
PROVENANCE_PATH = join(BASE_PATH, "processed_studies_provenance.csv")
# metagenomic_studies.csv should have two columns, one w the study name and the other the directory
ALL_STUDIES_PATH = join(BASE_PATH, "metagenomics_studies.csv")
MAG_CONFIG_PATH = join(BASE_PATH, "mag.config")
MAG_VERSION = "3.0.1"


# we should configure airflow to rerun this if the DAG changes too
# TODO once we add mag's friends, the processed studies file will
# need to be changed so that 'code_revision' becomes something more specific
# TODO should think about how this function relates to a hypothetical metatdenovo dag
# say we had both running over the same studies... do we have two processed_studies files?
# TODO i like these two functions, they should go in a utils file. 
# maybe we could use a StudyProvenanceProcessor class or something?
def load_processed_studies(provenance_path=PROVENANCE_PATH):
    """Load processed studies from a CSV file into a dictionary."""
    processed_studies = {}
    if os.path.exists(provenance_path):
        with open(provenance_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 'code_revision': row['code_revision']}
    return processed_studies

# TODO make sure this overwrites existing rows in the provenance file
# TODO swap to using this in the dag below
def update_provenance_file(study, timestamp, provenance_path=PROVENANCE_PATH, code_revision=MAG_VERSION):
    """Update the provenance file with the latest processing information."""
    fieldnames = ['study', 'timestamp', 'code_revision']
    new_data = {'study': study, 'timestamp': timestamp, 'code_revision': code_revision}
    with open(provenance_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.stat(provenance_path).st_size == 0:
            writer.writeheader()
        writer.writerow(new_data)

def create_dag():
    default_args = {
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
    }

    with DAG(
        dag_id="automated_mag_and_friends",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
        params={'clusterLogin': 'dcallan'}
    ) as dag:
        # this dag will run jobs on pmacs
        cluster_config = json.load(open(CLUSTER_CONFIG_PATH))
        cluster_manager = ClusterManager(
            cluster_config['headNode'], 
            cluster_config['fileTransferNode'], 
            cluster_config['clusterType'], 
            "{{params.clusterLogin}}"
        )

        # this should send the whole directory to the cluster. 
        # is that what we want? or just specific files in the directory?
        # TODO do we still want to do this if there are no studies to process?
        copy_config_to_cluster = cluster_manager.copyToCluster(
            BASE_PATH, 
            'mag.config', 
            '.', 
            gzip=False, 
            task_id = "copy_config_to_cluster"
        )        

        processed_studies = load_processed_studies()

        if os.path.exists(ALL_STUDIES_PATH):
            with open(ALL_STUDIES_PATH, "r") as file:
                next(file)  # Skip header
                for line in file:
                    studyName, studyPath = line.strip().split(",")
                    current_timestamp = os.path.getmtime(studyPath)
                    process_study = studyName not in processed_studies or \
                        processed_studies[studyName]['timestamp'] != str(current_timestamp) or \
                        processed_studies[studyName]['code_revision'] != MAG_VERSION

                    if process_study:
                        # make a task group for each study
                        with TaskGroup(f'{studyName}') as current_tasks:

                            # copy the study to the cluster
                            splitStudyPath = os.path.split(studyPath)
                            if (splitStudyPath[1] == ''):
                                splitStudyPath = splitStudyPath[0]
                            headStudyPath = splitStudyPath[0]
                            tailStudyPath = splitStudyPath[1]
                            copy_study_to_cluster = cluster_manager.copyToCluster(
                                headStudyPath, 
                                tailStudyPath, 
                                '.', 
                                gzip=False,
                                task_id = "copy_study_to_cluster",
                                task_group=current_tasks
                            )

                            accessionsFile = os.path.join(studyPath, "accessions.txt")
                            if os.path.exists(accessionsFile):
                                cmd = f"nextflow run nf-core/fetchngs -profile singularity --input {accessionsFile} --outdir {studyName}/data"
                                run_fetchngs = cluster_manager.startClusterJob(cmd, task_id="run_fetchngs", task_group=current_tasks)

                                # 900 seconds is 15 minutes, considered making it 5 min instead and still might
                                watch_fetchngs = cluster_manager.monitorClusterJob(
                                    run_fetchngs.output, 
                                    mode='reschedule', 
                                    poke_interval=900,
                                    task_id="watch_fetchngs",
                                    task_group=current_tasks
                                )

                                draft_samplesheet = os.path.join(studyName, "data/samplesheet/samplesheet.csv")
                                cmd = f"awk -F ',' -v OFS=',' '{{print $1,$4,$5,$2,$3}}' {draft_samplesheet}" \
                                        " | sed 1,1d | sed '1i sample,run,group,short_reads_1,short_reads_2' | sed 's/\"//g'"
                                make_mag_samplesheet = cluster_manager.startClusterJob(cmd, task_id="make_mag_samplesheet", task_group=current_tasks)

                                watch_make_mag_samplesheet = cluster_manager.monitorClusterJob(
                                    make_mag_samplesheet.output, 
                                    poke_interval=1,
                                    task_id="watch_make_mag_samplesheet",
                                    task_group=current_tasks
                                )
                            elif not os.path.exists(os.path.join(studyPath, "samplesheet.csv")):
                                raise Exception(f"No samplesheet.csv or accessions.txt found for {studyName} in {studyPath}")

                            # TODO figure out the reference dbs
                            # maybe like copying config, a step before all this to download manually
                            # should probably check if its already there, from a prev run too
                            cmd = ("nextflow run nf-core/mag -c mag.config " +
                                f"--input {studyName}/data/samplesheet.txt " +
                                f"--outdir {studyName}/out " +
                                "--skip_gtdbtk --skip_spades --skip_spadeshybrid --skip_concoct " +
                                "--kraken2_db \"k2_pluspf_20240112.tar.gz\" " +
                                "--genomad_db \"genomad_db\"")
                            run_mag = cluster_manager.startClusterJob(cmd, task_id="run_mag", task_group=current_tasks)

                            watch_mag = cluster_manager.monitorClusterJob(
                                run_mag.output, 
                                mode='reschedule', 
                                poke_interval=1800, 
                                task_id="watch_mag",
                                task_group=current_tasks
                            )

                            copy_results_from_cluster = cluster_manager.copyFromCluster(
                                '.', 
                                f"{studyName}/out", 
                                os.path.join(studyPath, 'out'), 
                                gzip=False,
                                task_id = "copy_results_from_cluster",
                                task_group=current_tasks
                            )

                            # this should make rda files for the r package via a dedicated Rscript
                            @task(task_group=current_tasks)
                            def post_process_results():
                                # TODO make sure the out path is correct
                                cmd = f"Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/mag_postProcessing.R {studyName} {studyPath}/out"
                                return(cmd)

                            # this should either update or add a row to the study provenance file
                            @task(task_group=current_tasks)
                            def update_provenance():
                                # this should find the row where 'study' == studyName
                                # and update its 'timestamp' and 'code_revision'
                                # or add a new row if it doesn't exist
                                current_timestamp = os.path.getmtime(studyPath)
                                with open(PROVENANCE_PATH, 'rw') as file:
                                    reader = csv.DictReader(file)
                                    writer = csv.writer(file)
                                    #ignore header
                                    next(file)

                                    for row in reader:
                                        if (row['study'] == studyName):
                                            row['timestamp'] = current_timestamp
                                            row['code_revision'] = MAG_VERSION
                                            writer.writerow(row)
                                            break
                                        else:
                                            writer.writerow([studyName, current_timestamp, MAG_VERSION])

                            if os.path.exists(accessionsFile):
                                copy_config_to_cluster >> \
                                copy_study_to_cluster >> \
                                run_fetchngs >> \
                                watch_fetchngs >> \
                                make_mag_samplesheet >> \
                                watch_make_mag_samplesheet >> \
                                run_mag >> \
                                watch_mag >> \
                                copy_results_from_cluster >> \
                                post_process_results() >> \
                                update_provenance()
                            else:
                                copy_config_to_cluster >> \
                                copy_study_to_cluster >> \
                                run_mag >> \
                                watch_mag >> \
                                copy_results_from_cluster >> \
                                post_process_results() >> \
                                update_provenance()

        else:
            raise FileNotFoundError(f"Studies file not found: {ALL_STUDIES_PATH}")

    return dag

# Define the DAG by calling the function
create_dag()
