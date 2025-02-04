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
import platform
import os
import datetime
from dotenv import load_dotenv

# Activate this on windows before importing pyesgf, otherwise import of logon manager throws error
if platform.uname().system == 'Windows':
    os.environ['HOME'] = os.environ['USERPROFILE'] 

from pyesgf.search import SearchConnection
from pyesgf.logon import LogonManager
# import xesmf as xe

os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "on"

logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARNING)

load_dotenv('../.config/credentials.env')

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

def load_chelsa_w5e5(variables, resolution, years, months = np.arange(1, 13), aoi = None, n_threads = 1, download_dir = TemporaryDirectory()):

    url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc" ##mode=bytes
    
    if isinstance(years, int):
        years = [years]
    if isinstance(months, int):
        months = [months]
    if (aoi is not None) and (not isinstance(aoi, tuple)):
        raise ValueError(f"aoi must be provided as tuple. Got {type(aoi)}")
    if not all([i in ['orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'] for i in variables]):
        raise ValueError(f"Variables must be one of 'orog', 'pr', 'rsds', 'tas', 'tasmax', 'tasmin'. Got {', '.join(variables)}")

    if (min(years) < 1979) or (max(years) > 2016):
        raise ValueError(f'years must fall within 1979-2016. Values outside this range are not supported. Got {", ".join(years)}')

    if aoi is not None:
        minx, miny, maxx, maxy = aoi
    else:
        minx, miny, maxx, maxy = (-180, -90, 180, 90)  #global bounding box
    
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
    
    logger.info('Loading data into dataset')
    # ds = xr.open_mfdataset(urls, chunks='auto', join = 'override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))
    # ds = xr.combine_by_coords([xr.open_dataset(i).sel(lat=slice(miny, maxy), lon=slice(minx, maxx)) for i in dwnloads], join = 'override', combine_attrs='override')
    ds = []
    for i in dwnloads:
        logger.debug(f'Loading {i.name}')
        ds.append(xr.open_dataset(i).sel(lat=slice(miny, maxy), lon=slice(minx, maxx)))
    ds = xr.combine_by_coords(ds, join = 'override', combine_attrs='override')

    for var in ds.keys():
        if 'tas' in var:
            logger.debug(f'Transforming data units for var {var}')
            ds[var] = ds[var] - 273.5
    ds = ds.rio.write_crs(4326)
    
    return(ds)

def load_cordex(
    user, pwd, 
    variables = ['tasAdjust', 'prAdjust', 'tasminAdjust', 'tasmaxAdjust'], 
    project = 'CORDEX-Adjust', 
    year_start = 1980, 
    year_end = 2100, 
    time_frequency = 'day',
    experiment = 'rcp45', 
    bias_adjustment = 'v1-LSCE-IPSL-CDFt-EOBS10-1971-2005'
):

    fnams, urls = query_cordex(user, pwd, variables, project, year_start, year_end, time_frequency, experiment, bias_adjustment)

    ds = xr.open_mfdataset(urls)[variables]#.sel(rlat = slice(-5, 0), rlon = slice(-10, -5))

    if len(set([i.shape for i in ds])) > 1:
        raise ValueError(f'Shape mismatch between variables!')
    ds = xr.merge(ds)

    return(ds)


def query_cordex(
    user, pwd, 
    variables = ['tasAdjust', 'prAdjust', 'tasminAdjust', 'tasmaxAdjust'], 
    project = 'CORDEX-Adjust', 
    year_start = 1980, 
    year_end = 2100, 
    time_frequency = 'day',
    experiment = 'rcp45', 
    bias_adjustment = 'v1-LSCE-IPSL-CDFt-EOBS10-1971-2005'
):

    # Log into ESGF Portal
    openid = f"https://esgf.nci.org.au/esgf-idp/openid/{user}"

    lm = LogonManager()

    if not lm.is_logged_on():
        lm.logon_with_openid(openid=openid, bootstrap=True, password=pwd)

    if not lm.is_logged_on():
        raise ValueError('Log on failed!')

    logger.debug('Logged into ESGF portal')

    # Execute Query
    search_args = {
        'project': project,
        'domain': 'EUR-11',
        'time_frequency': time_frequency,
        'from_timestamp': f"{year_start}-01-01T00:00:00Z",
        'to_timestamp': f"{year_end}-12-31T23:59:00Z",
        'ensemble': 'r1i1p1',
        'experiment': experiment,
        "bias_adjustment": bias_adjustment,
        'facets': 'driving_model,rcm_name,variable',
        'latest': True,
    }

    conn = SearchConnection('https://esgf-data.dkrz.de/esg-search', distrib=True)
    ctx = conn.new_context(**search_args)
    gcms = list(ctx.facet_counts['driving_model'].keys())
    logger.debug(f'Query executed. Found {len(gcms)} gcms.')

    fnams_all = []
    urls_all = []
    for gcm in gcms:
        ctx_gcm = ctx.constrain(driving_model = gcm)
        rcms = list(ctx_gcm.facet_counts['rcm_name'].keys())

        for rcm in rcms:
            ctx_rcm = ctx_gcm.constrain(rcm_name = rcm)
            vars_list = list(ctx_rcm.facet_counts['variable'].keys())

            if not all([i in vars_list for i in variables]):
                logger.warning(f"Not all variables present for {gcm}-{rcm}")
                continue

            for var in variables:
                ctx_var = ctx_rcm.constrain(variable = var)
                dataset = ctx_var.search()

                if len(dataset) != 1:
                    logger.warning(f'More than 1 files found for {gcm}-{rcm}-{var}! Found {len(dataset)} files.')
                    continue

                files = dataset[0].file_context().search()
                fnames = [i.filename for i in files]
                urls = [i.opendap_url for i in files]

                sdates = [datetime.datetime.strptime(i.split('_')[-1].split('-')[0], '%Y%m%d') for i in fnames]
                fnames_sel = [fnam for i,fnam in enumerate(fnames) if sdates[i] >= datetime.datetime(year_start, 1, 1)]
                urls_sel = [url for i,url in enumerate(urls) if sdates[i] >= datetime.datetime(year_start, 1, 1)]

                fnams_all.extend(fnames_sel)
                urls_all.extend(urls_sel)

                logger.debug(f"Found {len(urls_all)} files for {gcm}-{rcm}-{var}")

    return(fnams_all, urls_all)


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
    parser.add_argument('-a', '--aoi', default = 'europe', help = 'Name of area of interest for analysis.')
    parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')
    parser.add_argument('-ys', '--year_start', default = 2000, type = int, help = 'Starting year of climate grids.')
    parser.add_argument('-ye', '--year_end', default = 2001, type = int, help = 'Last year of climate grids. Must be greater than year_start.')
    parser.add_argument('-yc', '--year_chunks', default = 1, type = int, help = 'Number of years that should be processed at once. Depends on RAM of host. Default is to process each year individually.')
    parser.add_argument('-tr', '--threads', default = 1, type = int, help = 'Number of threads to use for downloading files')
    parser.add_argument('-d', '--ddir', default = TemporaryDirectory(), help = 'Directory to store downloaded files')

    args = parser.parse_args()

    minx, miny, maxx, maxy = aois[args.aoi]
    ds = load_chelsa_w5e5(
        args.variables,
        resolution=f"{args.resolution}arcsec",
        years=np.arange(args.year_start, args.year_end),
        months=np.arange(3, 13),
        aoi=(minx, miny, maxx, maxy),
        n_threads=args.threads,
        download_dir=args.ddir
    )

    logger.info(f'Downloaded dataset has the following shape: {list(ds.sizes.items())} and keys: {list(ds.keys())}')
