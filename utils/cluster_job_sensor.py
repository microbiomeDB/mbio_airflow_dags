from airflow.sensors.base_sensor_operator import BaseSensorOperator
import subprocess

class ClusterJobSensor(BaseSensorOperator):
    def __init__(self, jobId, sshTarget, clusterType, *kwargs):
        """
        Initializes the ClusterJobSensor.

        Args:
            jobId (str): a jobId from the cluster to monitor
            sshTarget (str): the ssh target (login@hostname)
            clusterType (str): the cluster type (LSF or SLURM)
            *kwargs: Variable length argument list

        Returns:
            None
        """
        self.jobId = jobId
        self.sshTarget = sshTarget
        self.clusterType = clusterType

        super().__init__(*kwargs)

    def poke(self):
        """
        Executes a command to check the status of a cluster job.

        Parameters:
            self (ClusterJobSensor): The instance of the ClusterJobSensor class.

        Returns:
            bool: False if the job is still running, True if the job has finished.
        """
        
        if (self.clusterType == "LSF"):        
            cmd = f"ssh -2 {self.sshTarget} 'bjobs {self.jobId}'"
        elif (self.clusterType == "SLURM"):
            cmd = f"ssh -2 {self.sshTarget} 'squeue -j {self.jobId}'"

        jobInfo = subprocess.run(cmd, shell=True)

        # TODO this likely is correct yet. its a placeholder
        # if the job is still running, return False
        if (jobInfo.returncode == 0):
            return False
        else:
            return True