import xarray as xr
import numpy as np
from itertools import product

from functions.base_logger import logger

def get_climate():
    pass

def load_chelsa_w5e5(variables, resolution, years, months = np.arange(1, 13), aoi = None):

    url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
    
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

    ##Load data into memory
    logger.debug('Loading data into Dataset')
    ds = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))

    if len(ds.chunks) > 0:
        logger.debug('Loading dataset into memory')
        ds = ds.compute()
    for var in ds.keys():
        logger.debug('Transforming data units')
        if 'tas' in var:
            ds[var] = ds[var] - 273.5

    return(ds)

def load_cordex():
    pass