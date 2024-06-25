from airflow.operators.bash_operator import BashOperator

# TODO consider implementation multiple constructors somehow, to accept a config file and login
# or maybe that should be the default constructor here, and grow into manual config
# TODO write a helper to read the json config file
# TODO validate inputs
class ClusterManager():
    _instance = None
    def __init__(self, headNode, fileTransferNode, clusterType, clusterLogin, **kwargs):
        self.headNode = headNode # from config
        self.fileTransferNode = fileTransferNode # from config
        self.clusterType = clusterType # from config
        self.clusterLogin = clusterLogin # user param

    def copyToCluster(self, fromDir, fromFile, toDir, gzip=False):
        # TODO validate inputs, all should be strings except gzip which should be boolean

        # TODO shouldnt be hardcoded path
        cmd = f"/data/MicrobiomeDB/mbio-airflow-dags/bin/copyToCluster.sh {fromDir} {fromFile} {toDir} {gzip} {self.clusterLogin}@{self.fileTransferNode}"

        return BashOperator(
            task_id='copyToCluster',
            bash_command=cmd
        )

    def copyFromCluster(self, fromDir, fromFile, toDir, deleteAfterCopy=False, gzip=False):
        # TODO validate inputs

        # TODO shouldnt be hardcoded path
        cmd = f"/data/MicrobiomeDB/mbio-airflow-dags/bin/copyFromCluster.sh {fromDir} {fromFile} {toDir} {gzip} {deleteAfterCopy} {self.clusterLogin}@{self.fileTransferNode}"

        return BashOperator(
            task_id='copyFromCluster',
            bash_command=cmd
        )

    # TODO double check the slurm variant
    def startClusterJob(self, command, logFile):

        if (self.clusterType == "LSF"):
            command = f"bsub {command}"
        elif (self.clusterType == "SLURM"):
            command = f"sbatch {command}"
        else:
            raise Exception(f"Cluster type {self.clusterType} not supported")

        cmd = f"ssh -2 {self.clusterLogin}@{self.headNode} '/bin/bash -login -c \"{command}\"'"

        # this grabs the job id returned from the command being submitted
        if (self.clusterType == "LSF"):
            cmd += "| awk -F '[<>]' '{print $2}'"
        elif (self.clusterType == "SLURM"):
            cmd += "| awk '{print $4}'"

        return BashOperator(
            task_id='startClusterJob',
            bash_command=cmd
        )

    # this should take the pid returned by the run command (or a dir to monitor, or something)
    # should know when the job is done running, using some sensor, so we know to trigger the copy back task
    def monitorClusterJob(self, jobId):
        return BashOperator(
            task_id='task4',
            bash_command='echo Im running task 4, the current execution date is {{ds}} and the previous execution date is {{prev_ds}}'
        )