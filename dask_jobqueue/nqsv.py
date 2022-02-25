import logging
import math
import os

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters
from .pbs import pbs_format_bytes_ceil

logger = logging.getLogger(__name__)


class NQSVJob(Job):
    submit_command = "qsub"
    cancel_command = "qdel"
    config_name = "nqsv"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if resource_spec is None:
            resource_spec = dask.config.get(
                "jobqueue.%s.resource-spec" % self.config_name
            )
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)
        if project is None:
            project = dask.config.get(
                "jobqueue.%s.project" % self.config_name
            ) or os.environ.get("PBS_ACCOUNT")

        # Try to find a project name from environment variable
        project = project or os.environ.get("PBS_ACCOUNT")

        header_lines = []
        # PBS header build
        if self.job_name is not None:
            header_lines.append("#PBS -N %s" % self.job_name)
        if queue is not None:
            header_lines.append("#PBS -q %s" % queue)
        if project is not None:
            header_lines.append("#PBS -A %s" % project)

        cpunum_lhost = "#PBS --cpunum-lhost=%d" % self.worker_cores
        header_lines.append(cpunum_lhost)
        memory_string = pbs_format_bytes_ceil(self.worker_memory)
        memsz_lhost = "#PBS --memsz-lhost=%s" % memory_string
        header_lines.append(memsz_lhost)

        if resource_spec is not None:
            header_lines.append("#PBS -l %s" % resource_spec)
        if walltime is not None:
            header_lines.append("#PBS -l elapstim_req=%s" % walltime)
        if self.log_directory is not None:
            header_lines.append("#PBS -e %s/" % self.log_directory)
            header_lines.append("#PBS -o %s/" % self.log_directory)
        header_lines.extend(["#PBS %s" % arg for arg in job_extra])

        # Declare class attribute that shall be overridden
        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())


class NQSVCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on a PBS cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#PBS -A` option.
    {job}
    {cluster}
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other PBS options. Each option will be prepended with the #PBS prefix.

    Examples
    --------
    >>> from dask_jobqueue import PBSCluster
    >>> cluster = PBSCluster(queue='regular', project="myproj", cores=24,
    ...     memory="500 GB")
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = NQSVJob
