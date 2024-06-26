import json
import pendulum

from airflow import DAG
from mbio_utils.cluster_manager import ClusterManager

with DAG(
        dag_id = "cluster_test",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        params={'clusterLogin': 'dcallan'}
) as dag:
    # TODO make this configurable by user somehow? and not hard coded..
    # ideally a user would just use the clusterName 'pmacs' and wed find the right config
    cluster_config = json.load(open('/data/MicrobiomeDB/mbio_airflow_dags/cluster_configs/pmacs.json'))
    cluster_manager = ClusterManager(
        cluster_config['headNode'], 
        cluster_config['fileTransferNode'], 
        cluster_config['clusterType'], 
        "{{params.clusterLogin}}"
    )

    # make a dummy file to test with
    open('test.txt', 'a').close()

    copyTestFileToCluster = cluster_manager.copyToCluster('.', 'test.txt', 'copy_test.txt', gzip=False)

    copyTestFileFromCluster = cluster_manager.copyFromCluster('copy_test.txt', 'copy_test.txt', '.')

    # TODO if this works then add a cluster job to add a line to the copy_test.txt file
    # TODO if that also works, make the task to add a line wait for a min first, to test monitoring the job

    copyTestFileToCluster >> copyTestFileFromCluster
