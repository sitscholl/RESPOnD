import dask
from dask.distributed import Client
import logging
import logging.config

logger = logging.getLogger(__name__)

def init_cluster(init_slurm = False, cluster_kwargs = dict(), n_jobs = 1):

    if init_slurm:
        from dask_jobqueue import SLURMCluster
        cluster = SLURMCluster(**cluster_kwargs)
        cluster.scale(jobs=n_jobs)
        # cluster.adapt(minimum=min_workers, maximum=max_workers)
    else:
        from dask.distributed import LocalCluster
        cluster = LocalCluster()

    client = Client(cluster)
    logger.info("Initialized Dask client on: %s", client)

    return(client)