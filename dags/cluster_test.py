import json
import pendulum

from include.src.date.decorator import DefaultDateTime
from airflow import DAG
from include.src.airflow.xcom import cleanup
from mbio_airflow_dags.utils.ClusterManager import ClusterManager

with DAG(
        dag_id = "cluster_test",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        on_failure_callback=cleanup,
        on_success_callback=cleanup
) as dag:
    # TODO make this configurable by user somehow? and not hard coded..
    # ideally a user would just use the clusterName 'pmacs' and wed find the right config
    cluster_config = json.load(open('/data/MicrobiomeDB/mbio-airflow-dags/cluster_configs/pmacs.json'))   
    cluster_manager = ClusterManager(
        cluster_config['headNode'], 
        cluster_config['fileTransferNode'], 
        cluster_config['clusterType'], 
        cluster_config['clusterLogin']
    )

    # make a dummy file to test with
    open('test.txt', 'a').close()

    copyTestFileToCluster = cluster_manager.copyToCluster('.', 'test.txt', 'copy_test.txt', gzip=False)

    copyTestFileFromCluster = cluster_manager.copyFromCluster('copy_test.txt', 'copy_test.txt', '.')

    # TODO if this works then add a cluster job to add a line to the copy_test.txt file
    # TODO if that also works, make the task to add a line wait for a min first, to test monitoring the job

    copyTestFileToCluster >> copyTestFileFromCluster