from airflow.sensors.base_sensor_operator import BaseSensorOperator
import subprocess
import sys

class ClusterJobSensor(BaseSensorOperator):
    template_fields = ('jobId', 'sshTarget')

    def __init__(self, task_id, jobId, sshTarget, clusterType, *default_args, **kwargs):
        """
        Initializes the ClusterJobSensor.

        Args:
            jobId (str): a jobId from the cluster to monitor (templated field).
            sshTarget (str): the ssh target (login@hostname)
            clusterType (str): the cluster type (LSF or SLURM)
            *default_args: Airflow default arguments
            **kwargs: Variable length key word argument list

        Returns:
            None
        """
        super().__init__(task_id = task_id, *default_args, **kwargs)
        
        self.jobId = jobId
        self.sshTarget = sshTarget
        self.clusterType = clusterType


    def poke(self, config):
        """
        Executes a command to check the status of a cluster job.

        Parameters:
            self (ClusterJobSensor): The instance of the ClusterJobSensor class.
            config (AirflowConfigParser): A thing for Airflow magic. 

        Returns:
            bool: False if the job is still running, True if the job has finished.
        """
       
        if (self.clusterType == "LSF"):        
            cmd = f"ssh -2 {self.sshTarget} 'bjobs {self.jobId}'"
        elif (self.clusterType == "SLURM"):
            cmd = f"ssh -2 {self.sshTarget} 'squeue -j {self.jobId}'"

        print(f"Monitoring job id: {self.jobId}", file=sys.stderr)
        jobInfo = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout
        jobStatus = jobInfo.split()[10]
        print(f"Found job status: {jobStatus}", file=sys.stderr)

        # TODO this likely isnt correct yet. its a placeholder
        # if the job is still running, return False
        if (jobStatus == 'RUN'):
            return False
        elif (jobStatus == 'PEND'):
            return False
        else:
            return True
