import pendulum
import csv
import os
import json
import sys

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from mbio_utils.cluster_manager import ClusterManager

with DAG(
    dag_id="automated_mag_and_friends",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    params={'clusterLogin': 'dcallan'},
) as dag:
    
    # this dag will run jobs on pmacs
    cluster_config = json.load(open('/data/MicrobiomeDB/mbio_airflow_dags/cluster_configs/pmacs.json'))
    cluster_manager = ClusterManager(
        cluster_config['headNode'], 
        cluster_config['fileTransferNode'], 
        cluster_config['clusterType'], 
        "{{params.clusterLogin}}"
    )

    # studies.csv should have two columns, one w the study name and the other the directory
    # we need to make a bash operator for each study

    # we should configure airflow to rerun this if the DAG changes too
    # TODO once we add mag's friends, the processed studies file will
    # need to be changed so that 'code_revision' becomes something more specific
    # TODO should think about how this task relates to a hypothetical metatdenovo dag
    # say we had both running over the same studies...
    mag_version = "3.0.1"
    # TODO make this a user param
    base_path = "/data/MicrobiomeDB/common/shotgun_metagenomics"

    # first see whats been run before and under what conditions
    # TODO refactor this since other dags will use it too
    # maybe as a Python class called ProcessStudies
    provenance_path = os.path.join(base_path, "processed_studies_provenance.csv")

    # this should send the whole directory to the cluster. 
    # is that what we want? or just specific files in the directory?
    copy_config_to_cluster = cluster_manager.copyToCluster(base_path, 'mag.config', '.', gzip=False)

    # this should read the studies.csv file, and compare the timestamp of the study to the one in the provenance file
    # it should make a dict of study name and path ready to run on the cluster
    @task
    def process_shotgun_metagenomic_studies():
        with open(provenance_path, 'r') as file:
            next(file)
            # Create a CSV DictReader
            reader = csv.DictReader(file)
    
            # i think we want a dict of dicts
            processed_studies = {}
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 'code_revision': row['code_revision']}
        
        # get study and path from csv
        # studyInfo should be a list of dicts of study name and path
        studyInfo = []
        all_studies_path = os.path.join(base_path, "metagenomics_studies.csv")
        with open(all_studies_path, "r") as file:
            next(file)
            for line in file:
                study, path = line.strip().split(",")
                current_timestamp = os.path.getmtime(path)
                processStudy = False
                if (study in processed_studies):
                    if ((current_timestamp > int(processed_studies[study]['timestamp'])) or
                        (mag_version != processed_studies[study]['code_revision'])):
                        processStudy = True
                else:
                    processStudy = True
                    
                if processStudy: 
                    studyInfo.append({'studyName': study, 'studyPath': path})

        return studyInfo

    # here the task group will run once for each study returned by process studies function
    @task_group(group_id="run_mag_and_friends_on_cluster")
    def mag_and_friends(studyName, studyPath):

        #TODO need to split studyPath, or make copyToCluster smarter
        # right now it tries to move to '.' and then copy an absolute path and it errs
        copy_study_to_cluster = cluster_manager.copyToCluster('.', studyPath, '.', gzip=False)

        #accessionsFile = f"{studyPath}/accessions.txt"
        #cmd = f"nextflow run nf-core/fetchngs -profile singularity --input {accessionsFile} --outdir {studyName}/data"
        #run_fetchngs = cluster_manager.startClusterJob(cmd)

        # 900 seconds is 15 minutes, considered making it 5 min instead and still might
        #watch_fetchngs = cluster_manager.monitorClusterJob(run_fetchngs.output, mode='reschedule', poke_interval=900)

        # TODO confirm location of draft samplesheet provided by fetchngs
        # TODO figure out what awk command to actually use for this
        # everything here is a placeholder currently
        #draft_samplesheet = f"{studyName}/data/samplesheet.csv"
        #cmd = f"cp {draft_samplesheet} {studyName}/data/samplesheet.txt"
        #make_mag_samplesheet = cluster_manager.startClusterJob(cmd)

        #watch_make_mag_samplesheet = cluster_manager.monitorClusterJob(make_mag_samplesheet.output, poke_interval=5)

        # TODO figure out the reference dbs
        # maybe like copying config, a step before all this to download manually
        # should probably check if its already there, from a prev run too
        cmd = ("nextflow run nf-core/mag -c mag.config " +
              f"--input {studyName}/data/samplesheet.txt " +
              f"--outdir {studyName}/out " +
               "--skip_gtdbtk --skip_spades --skip_spadeshybrid --skip_concoct " +
               "--kraken2_db \"k2_pluspf_20240112.tar.gz\" " +
               "--genomad_db \"genomad_db\"")
        run_mag = cluster_manager.startClusterJob(cmd)

        watch_mag = cluster_manager.monitorClusterJob(run_mag.output, mode='reschedule', poke_interval=1800)

        copy_results_from_cluster = cluster_manager.copyFromCluster('.', f"{studyName}/out", studyPath, gzip=False)

        # this should make rda files for the r package via a dedicated Rscript
        @task
        def post_process_results():
            # TODO make sure the out path is correct
            cmd = f"Rscript /data/MicrobiomeDB/mbio_airflow_dags/bin/mag_postProcessing.R {studyName} {studyPath}/out"
            return(cmd)

        # this should either update or add a row to the study provenance file
        @task
        def update_provenance():
            # this should find the row where 'study' == studyName
            # and update its 'timestamp' and 'code_revision'
            # or add a new row if it doesn't exist
            current_timestamp = os.path.getmtime(studyPath)
            with open(provenance_path, 'rw') as file:
                reader = csv.DictReader(file)
                writer = csv.writer(file)
                #ignore header
                next(file)

                for row in reader:
                    if (row['study'] == studyName):
                        row['timestamp'] = current_timestamp
                        row['code_revision'] = mag_version
                        writer.writerow(row)
                        break
                else:
                    writer.writerow([studyName, current_timestamp, mag_version])

        copy_study_to_cluster >> \
            run_mag >> \
            watch_mag >> \
            copy_results_from_cluster >> \
            post_process_results() >> \
            update_provenance()

    copy_config_to_cluster >> \
        mag_and_friends.expand_kwargs(process_shotgun_metagenomic_studies())

if __name__ == "__main__":
    dag.test()

