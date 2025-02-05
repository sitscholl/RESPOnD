import xarray as xr
import numpy as np
from itertools import product
import requests
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from multiprocessing.pool import ThreadPool
from functools import partial
import time
import logging

logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('positron_ipykernel').setLevel(logging.WARNING)

####Tests
####
# years = [2000]
# variables = ['tas']
# resolution = '1800arcsec'
# months = np.arange(1, 3)
# aoi = (-19.4, 27, 34.5, 57)
####
####

def get_climate():
    pass

def load_chelsa_w5e5(variables, resolution, years, months = np.arange(3, 13), n_threads = 1, download_dir = TemporaryDirectory(), **kwargs):

    url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc" ##mode=bytes
    
    if isinstance(years, int):
        years = [years]
    if isinstance(months, int):
        months = [months]

    if not all([i in ['orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'] for i in variables]):
        raise ValueError(f"Variables must be one of 'orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'. Got {', '.join(variables)}")

    if (min(years) < 1979) or (max(years) > 2016):
        raise ValueError(f'years must fall within 1979-2016. Values outside this range are not supported. Got {", ".join(years)}')

    ##Generate list of urls
    urls = []
    for var in variables:
        urls.extend(
            [url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
            for y,m in product(years, months)
            ]
        )

    ##Load data
    dwnloads = _multithreaded_download(urls, n_threads, download_dir = download_dir)
    dwnloads.sort()
    ds = open_climate_dataset(dwnloads, **kwargs)

    for var in ds.keys():
        if 'tas' in var:
            logger.debug(f'Transforming data units for var {var}')
            ds[var] = ds[var] - 273.5
    ds = ds.rio.write_crs(4326)
    
    return(ds)

def load_cordex():
    pass

##https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
def _download_files(url, download_dir, attempts = 3, overwrite = False):

    if isinstance(download_dir, TemporaryDirectory):
        local_filename = Path(download_dir.name, url.split('/')[-1])
    else:
        local_filename = Path(download_dir, url.split('/')[-1])

    if local_filename.is_file() and (not overwrite):
        return(local_filename, None)

    # logger.debug(f"Downloading {url} to {local_filename}")

    for attempt in range(attempts):  # Retry up to n times

        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()  # Raise an error for failed requests
                with open(local_filename, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            return (local_filename, None)  # Success, return early

        except requests.exceptions.RequestException as e:
            logger.debug(f"Retry {attempt + 1} for {url}: {e}")
            time.sleep(2 ** (attempt+1))
    return (local_filename, f"Failed after {attempts} retries")  # Fail after retries

def _multithreaded_download(urls, n_threads, download_dir):

    logger.info(f'Download of {len(urls)} files started using {n_threads} threads')
    start = time.time()

    dfunc = partial(_download_files, download_dir = download_dir)
    results = ThreadPool(n_threads).imap_unordered(dfunc, urls)

    local_files = []
    for fnam, error in results:
        if error is None:
            logger.info(f"{fnam} fetched after {time.time() - start:.2f}s")
            local_files.append(fnam)
        else:
            logger.error(f"Error fetching {fnam}: {error}")

    logger.info(f"Downloads finished. Elapsed Time: {time.time() - start:.2f}s" )
    return(local_files)


def open_climate_dataset(
    file_list,
    use_dask=False,
    chunks={"time": 52},
    aoi=(-180, -90, 180, 90),
    init_slurm=False,
    cluster_kwargs=None,
):
    """
    Open a list of NetCDF files into a single xarray Dataset.

    Parameters:
      file_list : list
          List of file paths
      use_dask : bool, default False
          If False, load each file fully into memory and combine using xr.combine_by_coords.
          If True, load the dataset lazily with dask by passing the chunks argument.
      chunks : dict, default {'time': 52}
          Chunking dictionary for dask.
      aoi : tuple
          A tuple (minx, miny, maxx, maxy) specifying a spatial sub-selection. If provided,
          each file will be sub-selected accordingly (assuming coordinates named 'lon' and 'lat').
      init_slurm : bool, default False
          If True and use_dask is True, initialize a SLURMCluster using dask-jobqueue.
      cluster_kwargs : dict or None, default None
          Additional keyword arguments for initializing the SLURMCluster.
          For example: dict(queue='normal', cores=8, memory='16GB', walltime="01:00:00")

    Returns:
      ds : xarray.Dataset
          The combined dataset (either loaded into memory or dask-backed).
    """

    logger.info('Loading data into dataset')
    if (not isinstance(aoi, tuple)):
        raise ValueError(f"aoi must be provided as tuple. Got {type(aoi)}")
    minx, miny, maxx, maxy = aoi

    # Initialize dask cluster
    if use_dask:
        import dask
        from dask.distributed import Client

        if init_slurm:
            from dask_jobqueue import SLURMCluster

            cluster = SLURMCluster(**cluster_kwargs)
            cluster.scale(jobs=2) ##TODO: Change this
        else:
            from dask.distributed import LocalCluster

            cluster = LocalCluster()

        client = Client(cluster)
        logger.info("Initialized Dask client on: %s", client)

        # Lazy loading with dask: use open_mfdataset with provided chunks.
        ds = xr.open_mfdataset(file_list, chunks=chunks, combine='by_coords', join='override', combine_attrs='override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))

    else:
        # Eagerly load all files: open each file fully into memory then combine.
        ds_list = []
        for f in file_list:
            logger.debug("Opening file: %s", f)
            ds_tmp = xr.open_dataset(f).sel(lat=slice(miny, maxy), lon=slice(minx, maxx))
            ds_list.append(ds_tmp)
        ds = xr.combine_by_coords(ds_list, join='override', combine_attrs='override')

    return ds


if __name__ == "__main__":
    ###For testing downloading of files
    
    import logging
    import logging.config

    logging.config.fileConfig(".config/logging.conf", disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    import argparse
    from config import aois

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--variables', default = ['tas'], nargs='+', help = 'Variables to download. Choose one or more of tas, tasmax, tasmin and pr')
    parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')
    parser.add_argument('-ys', '--year_start', default = 2000, type = int, help = 'Starting year of climate grids.')
    parser.add_argument('-ye', '--year_end', default = 2001, type = int, help = 'Last year of climate grids. Must be greater than year_start.')
    parser.add_argument('-yc', '--year_chunks', default = 1, type = int, help = 'Number of years that should be processed at once. Depends on RAM of host. Default is to process each year individually.')
    parser.add_argument('-tr', '--threads', default = 1, type = int, help = 'Number of threads to use for downloading files')
    parser.add_argument('-d', '--ddir', default = TemporaryDirectory(), help = 'Directory to store downloaded files')
    parser.add_argument('-da', '--use_dask', action = 'store_true', help = 'Use dask for opening .nc files')
    parser.add_argument('-ic', '--init_slurm', action = 'store_true', help = 'Initialize a SLURM-based dask cluster')
    parser.add_argument('-a', '--aoi', default = 'europe', help = 'Name of area of interest for analysis.')

    args = parser.parse_args()

    minx, miny, maxx, maxy = aois[args.aoi]
    ds = load_chelsa_w5e5(
        args.variables,
        resolution=f"{args.resolution}arcsec",
        years=np.arange(args.year_start, args.year_end),
        months=np.arange(3, 13),
        aoi=(minx, miny, maxx, maxy),
        n_threads=args.threads,
        download_dir=args.ddir,
        use_dask = args.use_dask,
        init_slurm = args.init_slurm
    )

    logger.info(f'Downloaded dataset has the following shape: {list(ds.sizes.items())} and keys: {list(ds.keys())}')

    if len(ds.chunks) > 0:
        logger.info('Computing array')
        ds = ds.compute()

    logger.info(f"Average values are: {' -- '.join([f"{i}: {ds[i].mean().item():.2f}" for i in ds])}")
