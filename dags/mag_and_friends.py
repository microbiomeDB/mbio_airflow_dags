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
##          It should also be noted that this DAG assumes nextflow and conda are installed on the cluster.
##
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# Constants for file paths
# TODO should some/ all of these be configurable?
CLUSTER_CONFIG_PATH = "/data/MicrobiomeDB/mbio_airflow_dags/cluster_configs/pmacs.json"
BASE_PATH = "/data/MicrobiomeDB/common/shotgun_metagenomics"
PROVENANCE_PATH = join(BASE_PATH, "processed_studies_provenance.csv")
# metagenomic_studies.csv should have two columns, one w the study name and the other the directory
ALL_STUDIES_PATH = join(BASE_PATH, "metagenomics_studies.csv")

MAG_VERSION = "3.0.1"
TAXPROFILER_VERSION = "1.1.8"
# the wget download method has a bug in version 1.12.0
# its fixed on dev, and we should use that for now
# switch back to a stable version once the fix is merged and released
FETCHNGS_VERSION = "dev"
METATDENOVO_VERSION = "1.0.1"

# TODO i like these two functions, they should go in a utils file. 
# maybe we could use a StudyProvenanceProcessor class or something?
def load_processed_studies(provenance_path=PROVENANCE_PATH):
    """Load processed studies from a CSV file into a dictionary."""
    processed_studies = {}
    if os.path.exists(provenance_path):
        with open(provenance_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                processed_studies[row['study']] = {'timestamp': row['timestamp'], 
                                                    'mag_revision': row['mag_revision'],
                                                    'metatdenovo_revision': row['metatdenovo_revision'],
                                                    'taxprofiler_revision': row['taxprofiler_revision']}
    return processed_studies

# TODO make sure this overwrites existing rows in the provenance file
# TODO swap to using this in the dag below
def update_provenance_file(
    study, 
    timestamp, 
    provenance_path=PROVENANCE_PATH, 
    mag_revision=MAG_VERSION,
    metatdenovo_revision=METATDENOVO_VERSION,
    taxprofiler_revision=TAXPROFILER_VERSION
):
    """Update the provenance file with the latest processing information."""
    fieldnames = ['study', 'timestamp', 'mag_revision', 'metatdenovo_revision', 'taxprofiler_revision']
    new_data = {
        'study': study, 
        'timestamp': timestamp, 
        'mag_revision': mag_revision,
        'metatdenovo_revision': metatdenovo_revision,
        'taxprofiler_revision': taxprofiler_revision
    }
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

        copy_pipeline_configs = TaskGroup("copy_pipeline_configs")
        manage_reference_dbs = TaskGroup("manage_reference_dbs")

        # TODO do we still want to do this if there are no studies to process?
        copy_mag_config_to_cluster = cluster_manager.copyToCluster(
            BASE_PATH, 
            'mag.config', 
            '.', 
            gzip=False, 
            task_id = "copy_mag_config_to_cluster",
            task_group = copy_pipeline_configs
        )

        copy_fetchngs_config_to_cluster = cluster_manager.copyToCluster(
            '/data/MicrobiomeDB/common',
            'fetchngs.config',
            '.',
            gzip=False,
            task_id = "copy_fetchngs_config_to_cluster",
            task_group = copy_pipeline_configs
        )

        copy_taxprofiler_config_to_cluster = cluster_manager.copyToCluster(
            BASE_PATH,
            'taxprofiler.config',
            '.',
            gzip=False,
            task_id = "copy_taxprofiler_config_to_cluster",
            task_group = copy_pipeline_configs
        )

        copy_metatdenovo_config_to_cluster = cluster_manager.copyToCluster(
            BASE_PATH,
            'metatdenovo.config',
            '.',
            gzip=False,
            task_id = "copy_metatdenovo_config_to_cluster",
            task_group = copy_pipeline_configs
        )

        copy_samplesheet_templates_to_cluster = cluster_manager.copyToCluster(
            BASE_PATH,
            'samplesheet_templates',
            '.',
            gzip=False,
            task_id = "copy_samplesheet_templates_to_cluster"
        )

        get_kraken_db = cluster_manager.startClusterJob(
            "if [[ ! -f k2_pluspf_20240112.tar.gz ]]; then wget https://genome-idx.s3.amazonaws.com/kraken/k2_pluspf_20240112.tar.gz; fi;",
            task_id = "get_kraken_db",
            do_xcom_push = False,
            task_group = manage_reference_dbs
        )

        get_genomad_db = cluster_manager.startClusterJob(
            "if [[ ! -d genomad_db ]]; then conda create -n genomad -c conda-forge -c bioconda genomad; conda activate genomad; genomad download-database .; conda deactivate; fi;",
            task_id = "get_genomad_db",
            do_xcom_push = False,
            task_group = manage_reference_dbs
        )

        # useful for host reads removal
        # TODO not all studies are human studies
        get_human_reference_genome = cluster_manager.startClusterJob(
            "if [[ ! -f hg38.fa.gz ]]; then wget https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.fa.gz; fi;",
            task_id = "get_human_reference_genome",
            do_xcom_push = False,
            task_group = manage_reference_dbs
        )

        get_metaphlan_db = cluster_manager.startClusterJob(
            "if [[ ! -d metaphlan/metaphlan_databases ]]; then mkdir -p metaphlan/metaphlan_databases; wget http://cmprod1.cibio.unitn.it/biobakery4/metaphlan_databases/mpa_vJun23_CHOCOPhlAnSGB_202307.tar; tar -xvf mpa_vJun23_CHOCOPhlAnSGB_202307.tar -C metaphlan/metaphlan_databases; fi;",
            task_id = "get_metaphlan_db",
            do_xcom_push = False,
            task_group = manage_reference_dbs
        )

        get_mOTU_db = cluster_manager.startClusterJob(
            "if [[ ! -f db_mOTU_v3.0.1.tar.gz ]]; then wget https://zenodo.org/records/5140350/files/db_mOTU_v3.0.1.tar.gz; fi;",
            task_id = "get_mOTU_db",
            do_xcom_push = False,
            task_group = manage_reference_dbs
        )

        # this for taxpasta
        get_taxdump = cluster_manager.startClusterJob(
            "if [[ ! -d taxdump ]]; then wget wget https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz; tar -xvf new_taxdump.tar.gz -C taxdump; fi;",
            task_id = "get_taxdump",
            do_xcom_push = False,
            task_group = manage_reference_dbs
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
                        processed_studies[studyName]['mag_revision'] != MAG_VERSION or \
                        processed_studies[studyName]['metatdenovo_revision'] != METATDENOVO_VERSION or \
                        processed_studies[studyName]['taxprofiler_revision'] != TAXPROFILER_VERSION

                    if process_study:
                        # make a task group for each study
                        with TaskGroup(f'{studyName}') as current_tasks:

                            # copy the study to the cluster
                            # TODO should ignore previous work dir if it exists
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

                            accessionsFile = os.path.join(studyPath, "accessions.tsv")
                            if os.path.exists(accessionsFile):
                                cmd = (f"mkdir -p {tailStudyPath}/fetchngs_logs; " +
                                        f"cd {tailStudyPath}/fetchngs_logs; " +
                                        f"nextflow run nf-core/fetchngs " +
                                        f"--input ~/{tailStudyPath}/accessions.tsv " +
                                        f"--outdir ~/{tailStudyPath}/data " +
                                        f"-r {FETCHNGS_VERSION} " +
                                        "-c ~/fetchngs.config")
                                run_fetchngs = cluster_manager.startClusterJob(cmd, task_id="run_fetchngs", task_group=current_tasks)

                                # 900 seconds is 15 minutes, considered making it 5 min instead and still might
                                watch_fetchngs = cluster_manager.monitorClusterJob(
                                    run_fetchngs.output, 
                                    mode='reschedule', 
                                    poke_interval=900,
                                    task_id="watch_fetchngs",
                                    task_group=current_tasks
                                )

                                draft_samplesheet = os.path.join(tailStudyPath, "data/samplesheet/samplesheet.csv")
                                
                                cmd = (f'cut -d, -f 1,4,5,2,3 {draft_samplesheet} ' +
                                        ' | sed 1,1d | cat samplesheet_templates/mag_samplesheet_template.csv - ' +
                                        f' > {tailStudyPath}/mag_samplesheet.csv')
                                make_mag_samplesheet = cluster_manager.startClusterJob(cmd, task_id="make_mag_samplesheet", task_group=current_tasks)

                                watch_make_mag_samplesheet = cluster_manager.monitorClusterJob(
                                    make_mag_samplesheet.output, 
                                    poke_interval=1,
                                    task_id="watch_make_mag_samplesheet",
                                    task_group=current_tasks
                                )

                                cmd = (f'cut -d, -f 1,4,21,2,3 {draft_samplesheet} ' +
                                        ' | sed 1,1d | cat samplesheet_templates/taxprofiler_samplesheet_template.csv - ' +
                                        f' > {tailStudyPath}/taxprofiler_samplesheet.csv')
                                make_taxprofiler_samplesheet = cluster_manager.startClusterJob(
                                    cmd, 
                                    task_id="make_taxprofiler_samplesheet", 
                                    task_group=current_tasks
                                )

                                watch_make_taxprofiler_samplesheet = cluster_manager.monitorClusterJob(
                                    make_taxprofiler_samplesheet.output, 
                                    poke_interval=1,
                                    task_id="watch_make_taxprofiler_samplesheet",
                                    task_group=current_tasks
                                )

                                cmd = (f'cut -d, -f 1,2,3 {draft_samplesheet} ' +
                                        ' | sed 1,1d | cat samplesheet_templates/metatdenovo_samplesheet_template.csv - ' +
                                        ' > {tailStudyPath}/metatdenovo_samplesheet.csv')
                                make_metatdenovo_samplesheet = cluster_manager.startClusterJob(
                                    cmd, 
                                    task_id="make_metatdenovo_samplesheet", 
                                    task_group=current_tasks
                                )

                                watch_make_metatdenovo_samplesheet = cluster_manager.monitorClusterJob(
                                    make_metatdenovo_samplesheet.output, 
                                    poke_interval=1,
                                    task_id="watch_make_metatdenovo_samplesheet",
                                    task_group=current_tasks
                                )

                            elif not os.path.exists(os.path.join(studyPath, "mag_samplesheet.csv")):
                                raise Exception(f"No *samplesheet.csv or accessions.tsv found for {studyName} in {studyPath}")

                            cmd = (f"mkdir -p {tailStudyPath}/out/taxprofiler_logs; " +
                                    f"cd {tailStudyPath}/out/taxprofiler_logs; " +
                                    "nextflow run nf-core/taxprofiler " +
                                    f"-c ~/taxprofiler.config " +
                                    f"-r {TAXPROFILER_VERSION} " +
                                    f"--input ~/{tailStudyPath}/taxprofiler_samplesheet.csv " +
                                    f"--databases ~/{tailStudyPath}/taxprofiler_databases.csv " +
                                    f"--outdir ~/{tailStudyPath}/out/taxprofiler_out " +
                                    f"-work-dir ~/{tailStudyPath}/work/taxprofiler_work " +
                                    f"-params-file ~/{tailStudyPath}/taxprofiler-params.json")
                            run_taxprofiler = cluster_manager.startClusterJob(cmd, task_id="run_taxprofiler", task_group=current_tasks)

                            watch_taxprofiler = cluster_manager.monitorClusterJob(
                                run_taxprofiler.output, 
                                mode='reschedule', 
                                poke_interval=1800, 
                                task_id="watch_taxprofiler",
                                task_group=current_tasks
                            )

                            # TODO maybe make a constant for ref db names?
                            # TODO should clean up its work dir when its done
                            # wed move to that subdir of the study dir before launching these types of commands
                            cmd = (f"mkdir -p {tailStudyPath}/out/mag_logs; " +
                                    f"cd {tailStudyPath}/out/mag_logs; " +
                                    "nextflow run nf-core/mag " +
                                    "-c ~/mag.config " +
                                    f"--input ~/{tailStudyPath}/mag_samplesheet.csv " +
                                    f"--outdir ~/{tailStudyPath}/out/mag_out " +
                                    f"-work-dir ~/{tailStudyPath}/work/mag_work " +
                                    f"-r {MAG_VERSION} " +
                                    "--skip_gtdbtk --skip_spades --skip_spadeshybrid --skip_concoct " +
                                    "--kraken2_db ~/k2_pluspf_20240112.tar.gz " +
                                    "--genomad_db ~/genomad_db")
                            run_mag = cluster_manager.startClusterJob(cmd, task_id="run_mag", task_group=current_tasks)

                            watch_mag = cluster_manager.monitorClusterJob(
                                run_mag.output, 
                                mode='reschedule', 
                                poke_interval=1800, 
                                task_id="watch_mag",
                                task_group=current_tasks
                            )

                            cmd = (f"mkdir -p {tailStudyPath}/out/metatdenovo_logs; " +
                                    f"cd {tailStudyPath}/out/metatdenovo_logs; " +
                                    "nextflow run nf-core/metatdenovo " +
                                    f"-r {METATDENOVO_VERSION} " +
                                    f"-work-dir ~/{tailStudyPath}/work/metatdenovo_work " +
                                    f"--input ~/{tailStudyPath}/metatdenovo_samplesheet.csv " +
                                    f"--outdir ~/{tailStudyPath}/out/metatdenovo_out " +
                                    f"-params-file ~/{tailStudyPath}/metatdenovo-params.json " +
                                    "-c ~/metatdenovo.config"
                            )
                            run_metatdenovo = cluster_manager.startClusterJob(cmd, task_id="run_metatdenovo", task_group=current_tasks)

                            watch_metatdenovo = cluster_manager.monitorClusterJob(
                                run_metatdenovo.output, 
                                mode='reschedule', 
                                poke_interval=1800, 
                                task_id="watch_metatdenovo",
                                task_group=current_tasks
                            )                        

                            copy_results_from_cluster = cluster_manager.copyFromCluster(
                                tailStudyPath, 
                                "out", 
                                studyPath,
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

                                fieldnames = ['study', 'timestamp', 'mag_revision', 'metatdenovo_revision', 'taxprofiler_revision']
                                # Update or append new data
                                with open(PROVENANCE_PATH, 'w', newline='') as csvfile:
                                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                                    writer.writeheader()
                                    
                                    updated_study = {
                                        'study': studyName,
                                        'timestamp': current_timestamp,
                                        'mag_revision': MAG_VERSION,
                                        'taxprofiler_revision': TAXPROFILER_VERSION,
                                        'metatdenovo_revision': METATDENOVO_VERSION
                                    }

                                    # Merge with existing data
                                    all_data = {d['study']: d for d in processed_studies}
                                    all_data.update({studyName: updated_study})

                                    # Write back to the file
                                    writer.writerows(all_data.values())

                            copy_study_to_cluster >> \
                            run_mag >> \
                            watch_mag >> \
                            copy_results_from_cluster >> \
                            post_process_results() >> \
                            update_provenance()

                            copy_study_to_cluster >> \
                            run_metatdenovo >> \
                            watch_metatdenovo >> \
                            copy_results_from_cluster 
                            
                            copy_study_to_cluster >> \
                            run_taxprofiler >> \
                            watch_taxprofiler >> \
                            copy_results_from_cluster

                            if os.path.exists(accessionsFile):
                                copy_study_to_cluster >> \
                                run_fetchngs >> \
                                watch_fetchngs >> \
                                make_mag_samplesheet >> \
                                watch_make_mag_samplesheet >> \
                                run_mag

                                watch_fetchngs >> \
                                make_taxprofiler_samplesheet >> \
                                watch_make_taxprofiler_samplesheet >> \
                                run_taxprofiler

                                watch_fetchngs >> \
                                make_metatdenovo_samplesheet >> \
                                watch_make_metatdenovo_samplesheet >> \
                                run_metatdenovo

                            copy_pipeline_configs >> current_tasks
                            copy_samplesheet_templates_to_cluster >> current_tasks
                            manage_reference_dbs >> current_tasks

        else:
            raise FileNotFoundError(f"Studies file not found: {ALL_STUDIES_PATH}")

    return dag

# Define the DAG by calling the function
create_dag()
