from multiprocessing.pool import ThreadPool
from time import time as timer
from itertools import product
import numpy as np
from pydist.get_climate import _download_files
from tempfile import TemporaryDirectory
from functools import partial
import logging
import logging.config
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('threads', default = 1, type = int, help = 'Number of threads')
parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')

args = parser.parse_args()
n_threads = args.threads
res = f"{args.resolution}arcsec"

logging.config.fileConfig(".config/logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info(f'Program started using {n_threads} threads')

url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc" ##mode=bytes

urls = []
for var in ['tas', 'tasmin', 'tasmax', 'pr']:
    urls.extend(
        [url_template.format(variable=var, resolution=res, timestamp=f"{y}{m:02}")
        for y,m in product([2000], np.arange(3, 13))
        ]
    )

with TemporaryDirectory() as tempdir:
    start = timer()
    dfunc = partial(_download_files, download_dir = tempdir)
    results = ThreadPool(n_threads).imap_unordered(dfunc, urls)

    for fnam, error in results:
        if error is None:
            logger.info("%r fetched after %ss" % (fnam, timer() - start))
        else:
            logger.error("error fetching %r: %s" % (fnam, error))
            
    logger.info("Elapsed Time: %s" % (timer() - start,))