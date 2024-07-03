import json
import pendulum
import os

from airflow import DAG
from mbio_utils.cluster_manager import ClusterManager

with DAG(
        dag_id = "cluster_test",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        params={'clusterLogin': 'dcallan'},
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

    copyTestFileToCluster = cluster_manager.copyToCluster('/data/MicrobiomeDB/common/amplicon_sequencing/test_study/', 'copyToClusterTestFile.txt', '.', gzip=False)

    modifyTestFileOnCluster = cluster_manager.startClusterJob("sleep 30s; echo \"foobar\" > copyToClusterTestFile.txt")

    monitorTestJobOnCluster = cluster_manager.monitorClusterJob(modifyTestFileOnCluster.output, poke_interval=5)

    copyTestFileFromCluster = cluster_manager.copyFromCluster('.', 'copyToClusterTestFile.txt', '/data/MicrobiomeDB/common/amplicon_sequencing/test_study/')

    # the relationships of these tasks needs set explicitly
    copyTestFileToCluster >> modifyTestFileOnCluster >> monitorTestJobOnCluster >> copyTestFileFromCluster
