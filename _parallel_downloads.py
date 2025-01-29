from multiprocessing.pool import ThreadPool
from time import time as timer
from itertools import product
import numpy as np
from pydist.get_climate import load_chelsa_w5e5
from pydist import config
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
minx, miny, maxx, maxy = config.aois['europe']

logging.config.fileConfig(".config/logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

logger.info(f'Program started using {n_threads} threads')

ds = load_chelsa_w5e5(['tas', 'pr', 'tasmin', 'tasmax'], resolution = res, years = [2000], months = np.arange(3, 13), aoi = (minx, miny, maxx, maxy), n_threads = n_threads)
