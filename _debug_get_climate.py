from functions.get_climate import load_chelsa_w5e5
import argparse
from functions import config
from functions.base_logger import logger
import numpy as np
from dask.distributed import Client, LocalCluster

if __name__ == "__main__":
    cluster = LocalCluster()
    client = Client(cluster)

    ##Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--variables', default = ['tas'], nargs='+', help = 'Variables to download. Choose one or more of tas, tasmax, tasmin and pr')
    parser.add_argument('-a', '--aoi', default = 'europe', help = 'Name of area of interest for analysis.')
    parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')
    parser.add_argument('-ys', '--year_start', default = 2000, type = int, help = 'Starting year of climate grids.')
    parser.add_argument('-ye', '--year_end', default = 2001, type = int, help = 'Last year of climate grids. Must be greater than year_start.')
    parser.add_argument('-l', '--load', action = 'store_true', help = 'Load chunks into memory')

    args = parser.parse_args()

    if args.aoi not in list(config.aois.keys()):
        raise ValueError(f"Invalid input for aoi argument. Choose one of {', '.join(list(config.aois.keys()))}")
    minx, miny, maxx, maxy = config.aois[args.aoi]

    if args.resolution not in [30, 90, 300, 1800]:
        raise ValueError(f"Invalid input for resolution argument. Choose one of: 30, 90, 300, 1800")
    resolution = f"{args.resolution}arcsec"

    if args.year_start > args.year_end:
        raise ValueError('year_start must be greater than year_end!')
    if any([args.year_start < 1979, args.year_end < 1979, args.year_start > 2016, args.year_end > 2016]):
        raise ValueError('year_start and year_end arguments must both fall within 1979-2016. Values outside this range are not supported.')
    years = np.arange(args.year_start, args.year_end+1)

    variables = args.variables

    logger.info('Program started!')
    ds = load_chelsa_w5e5(variables, resolution, years, months = np.arange(3, 13), aoi = (minx, miny, maxx, maxy))

    if args.load and (len(ds.chunks) > 0):
        logger.debug('Loading dataset into memory')
        ds = ds.compute()

    logger.info(f'Execution finished! The following data was downloaded:')
    for var in ds:
        logger.info(f"{var}: {ds[var].shape} {ds[var].dims}")
