import xarray as xr
import numpy as np
from itertools import product
import requests
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from multiprocessing.pool import ThreadPool
from functools import partial
from time import time as timer
import logging

logger = logging.getLogger(__name__)

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

def load_chelsa_w5e5(variables, resolution, years, months = np.arange(1, 13), aoi = None, n_threads = 1):

    url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc" ##mode=bytes
    
    if isinstance(years, int):
        years = [years]
    if isinstance(months, int):
        months = [months]
    if not isinstance(aoi, tuple):
        raise ValueError(f"aoi must be provided as tuple. Got {type(aoi)}")
    if not all([i in ['orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'] for i in variables]):
        raise ValueError(f"Variables must be one of 'orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'. Got {', '.join(variables)}")

    if (min(years) < 1979) or (max(years) > 2016):
        raise ValueError(f'years must fall within 1979-2016. Values outside this range are not supported. Got {", ".join(years)}')
    
    minx, miny, maxx, maxy = aoi

    ##Generate list of urls
    urls = []
    for var in variables:
        urls.extend(
            [url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
            for y,m in product(years, months)
            ]
        )

    ##Load data
    logger.debug('Downloading files')
    with TemporaryDirectory() as tempdir:
        dwnloads = _multithreaded_download(urls, n_threads, download_dir = tempdir)
        
        logger.debug('Loading data into Dataset')
        logger.debug(f"Downloaded files: {';'.join([str(i) for i in dwnloads])}")
        #ds = xr.open_mfdataset(urls, chunks='auto', join = 'override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))
        ds = xr.combine_by_coords([xr.open_dataset(i) for i in dwnloads], join = 'override', combine_attrs='override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))

    for var in ds.keys():
        logger.debug('Transforming data units')
        if 'tas' in var:
            ds[var] = ds[var] - 273.5
    ds = ds.rio.write_crs(4326)
    
    return(ds)

def load_cordex():
    pass

##https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
def _download_files(url, download_dir):

    local_filename = Path(download_dir, url.split('/')[-1])
    logger.debug(f"Downloading {url} to {local_filename}")

    try:
        with requests.get(url, stream=True) as r:
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        return (local_filename, None)

    except Exception as e:
        return(local_filename, e)

def _multithreaded_download(urls, n_threads, download_dir):

    logger.info(f'Download of {len(urls)} files started using {n_threads} threads')
    start = timer()

    dfunc = partial(_download_files, download_dir = download_dir)
    results = ThreadPool(n_threads).imap_unordered(dfunc, urls)

    local_files = []
    for fnam, error in results:
        if error is None:
            logger.info(f"{fnam} fetched after {timer() - start:.2f}s")
            local_files.append(fnam)
        else:
            logger.error(f"Error fetching {fnam}: {error}")
            
    logger.info(f"Downloads finished. Elapsed Time: {timer() - start:.2f}s" )
    return(local_files)