from airflow.operators.bash_operator import BashOperator
from mbio_utils.cluster_job_sensor import ClusterJobSensor
from airflow.models.taskinstance import TaskInstance

# TODO consider implementation multiple constructors somehow, to accept a config file and login
# or maybe that should be the default constructor here, and grow into manual config
# TODO write a helper to read the json config file
# TODO validate inputs
class ClusterManager():
    _instance = None
    def __init__(self, headNode, fileTransferNode, clusterType, clusterLogin, **kwargs):
        """
        Initializes a new instance of the ClusterManager class.

        Args:
            headNode (str): The head node of the cluster.
            fileTransferNode (str): The file transfer node of the cluster.
            clusterType (str): The type of the cluster (e.g., LSF, SLURM).
            clusterLogin (str): The login information for the cluster.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        self.headNode = headNode # from config
        self.fileTransferNode = fileTransferNode # from config
        self.clusterType = clusterType # from config
        self.clusterLogin = clusterLogin # user param

    def copyToCluster(self, fromDir, fromFile, toDir, gzip=False, task_id = 'copyToCluster', **kwargs):
        """
        Copies a file to the cluster. The file will be gzipped if gzip is set to True.

        Args:
            fromDir (str): The directory where the file is located on the local machine.
            fromFile (str): The name of the file to be copied.
            toDir (str): The directory where the file will be copied on the cluster.
            gzip (bool, optional): Whether to gzip the file. Defaults to False.

        Returns:
            None
        """
        # TODO validate inputs, all should be strings except gzip which should be boolean

        # TODO shouldnt be hardcoded path
        cmd = f"/data/MicrobiomeDB/mbio_airflow_dags/bin/copyToCluster.sh {fromDir} {fromFile} {toDir} {gzip} {self.clusterLogin}@{self.fileTransferNode}"

        return BashOperator(
            task_id=task_id,
            bash_command=cmd,
            **kwargs
        )

    def copyFromCluster(self, fromDir, fromFile, toDir, deleteAfterCopy=False, gzip=False, task_id = 'copyFromCluster', **kwargs):
        """
        Copies a file from the cluster. The file will be gzipped if gzip is set to True.

        Args:
            fromDir (str): The directory where the file is located on the cluster.
            fromFile (str): The name of the file to be copied.
            toDir (str): The directory where the file will be copied on the local machine.
            deleteAfterCopy (bool, optional): Whether to delete the file from the cluster after it has been copied. Defaults to False.
            gzip (bool, optional): Whether to gzip the file. Defaults to False.

        Returns:
            None
        """
        # TODO validate inputs

        # TODO shouldnt be hardcoded path
        cmd = f"/data/MicrobiomeDB/mbio_airflow_dags/bin/copyFromCluster.sh {fromDir} {fromFile} {toDir} {gzip} {deleteAfterCopy} {self.clusterLogin}@{self.fileTransferNode}"

        return BashOperator(
            task_id=task_id,
            bash_command=cmd,
            **kwargs
        )

    # TODO double check the slurm variant
    def startClusterJob(self, command, task_id = 'startClusterJob', **kwargs):
        '''
        Starts a job on the cluster. Pushes the resulting jobId
        to Airflow Xcom.

        Args:
            command (str): The command to be executed on the cluster.

        Returns:
            None
        '''

        if (self.clusterType == "LSF"):
            command = f"bsub \\\"{command}\\\""
        elif (self.clusterType == "SLURM"):
            command = f"sbatch \\\"{command}\\\""
        else:
            raise Exception(f"Cluster type {self.clusterType} not supported")

        cmd = f"ssh -2 {self.clusterLogin}@{self.headNode} '/bin/bash -login -c \"{command}\"'"

        # this grabs the job id returned from the command being submitted
        if (self.clusterType == "LSF"):
            cmd += "| awk -F '[<>]' '{print $2}'"
        elif (self.clusterType == "SLURM"):
            cmd += "| awk '{print $4}'"

        return BashOperator(
            task_id=task_id,
            bash_command=cmd,
            **kwargs
        )

    # this should take the pid returned by the run command (or a dir to monitor, or something)
    # should know when the job is done running, using some sensor, so we know to trigger the copy back task
    def monitorClusterJob(self, jobId, task_id = 'monitorClusterJob', **kwargs):
        '''
        Monitors a job on the cluster to see if it has completed. 

        Args:
            jobId (str): pid of job to monitor on cluster (supports templated fields).

        Returns:
            None
        '''

        return ClusterJobSensor(
            task_id=task_id,
            jobId=jobId,
            sshTarget=f"{self.clusterLogin}@{self.headNode}",
            clusterType=self.clusterType,
            **kwargs
        )
