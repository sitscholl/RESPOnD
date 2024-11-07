import pandas as pd
# import geopandas as gpd
import numpy as np
import xarray as xr
# from rasterio.enums import Resampling
import matplotlib.pyplot as plt
from itertools import product
import logging

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logger.debug('Starting script')

###
tbl_parker = pd.read_csv('prepared_data/parker_2013.csv').drop('Unnamed: 0', axis = 1).dropna(subset = 'Prime Name')
tbl_candiago = pd.read_csv('prepared_data/candiago_2022.csv').drop('Unnamed: 0', axis = 1)
# candiago_shp = gpd.read_file('prepared_data/EU_PDO.gpkg')
parker_sub = (
    tbl_parker[["Prime Name", "F*"]]
    .copy()
    .loc[tbl_parker["Prime Name"].isin(tbl_candiago["Prime Name"].unique())]
    .dropna(subset="Prime Name")
)

#-19.423828,27.059126,34.541016,57.468589
aoi_lat = slice(27, 57) #slice(46, 47) #slice(44, 48)
aoi_lon = slice(-19.4, 34.5) #slice(10, 12) #slice(4, 16)
resolution = "90arcsec"
years = np.arange(2000, 2003)
months = np.arange(3, 12)
variables = ['tas', 'tasmin', 'tasmax', 'pr']
veraison_max = 275
veraison_min = 214
veraison_window = 45

url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
urls = []
for var in variables:
    urls.extend(
        [url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
         for y,m in product(years, months)
        ]
    )
logger.debug('Loading data into Dataset')
ds_clim = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=aoi_lat, lon=aoi_lon)

##Calculate temperature cumsum for each year after 60th doy
logger.debug('Calculating temperature cumulative sum')
tas_sum = (
    (
        ds_clim.tas.sel(time=(ds_clim.time.dt.dayofyear >= 60))# & (ds.time.dt.year <= 1983))
        - 273.5
    )
    .clip(min=0)
    .groupby("time.year")
    .cumsum()
    .compute()
)

##Load subset of ds_clim into memory starting from first to last veraison date to speed up calculation of indices
logger.debug('Loading subset of ds_clim into memory')
ds_clim_sub = ds_clim.sel(time = (ds_clim.time.dt.dayofyear >= veraison_min) & (ds_clim.time.dt.dayofyear <= (veraison_max + veraison_window))).compute()
logger.debug('K to °C')
for v in ['tas', 'tasmin', 'tasmax']:
    ds_clim_sub[v] = ds_clim_sub[v] - 273.5

clim_idx = []
for v_name, Fcrit in list(zip(parker_sub['Prime Name'], parker_sub['F*'])):

    logger.info(f'Starting variety {v_name}!')

    ##Drop days where all pixels are below Fcrit
    logger.debug('Droping days below Fcrit')
    tas_sub = tas_sum.where(tas_sum >= Fcrit)
    times_sel = ~tas_sub.isnull().all(['lat', 'lon'])
    del tas_sub

    ##Subtract Fcrit
    logger.debug('Calculating temperature difference')
    tas_diff = tas_sum.sel(time = times_sel) - Fcrit
    tas_diff = tas_diff.where(tas_diff >= 0)

    ##Get date when Fcrit is reached for each year
    logger.debug('Getting veraison date for each year')
    veraison_date = tas_diff.groupby('time.year').apply(lambda c: c.idxmin(dim="time"))

    ##Find dates that are within veraison_min and veraison_max
    logger.debug('Masking veraison date')
    veraison_date = veraison_date.where((veraison_date.dt.dayofyear < veraison_max) & (veraison_date.dt.dayofyear >= veraison_min))

    ##Create a mask containing all pixels that have a veraison date
    logger.debug('Creating valid mask')
    mask_veraison = veraison_date.groupby('year').apply(lambda x: x.notnull().any('year'))

    ##Expand array to add 45 days after Fcrit is reached
    logger.debug('Expanding array')
    # test4 = test3.expand_dims(nr = 1).interp(nr = np.arange(veraison_window)+1)
    veraison_range = xr.concat(
        [
            (veraison_date + np.timedelta64(i, "D")).assign_coords({"nr": i})
            for i in np.arange(veraison_window)
        ],
        dim="nr",
    )

    ##Remove dates that 'jumped to' next year (when using max. threshold for veraison_date, this should not be needed anymore??)
    # Fcrit_range = Fcrit_range.where(Fcrit_range.dt.month > 3)

    ##Check if required dates are present
    if ds_clim_sub.time.min() > veraison_range.min(skipna = True):
        raise ValueError(f'Timerange in ds_clim too small! Make sure to include additional months at start of year to cover entire veraison_range. ds_clim_sub starts at {ds_clim_sub.time.min().values} and veraison_range starts at {veraison_range.min(skipna = True).values}')

    ##Get climatic variables within 45 day window for valid pixels
    logger.debug('Extracting climatic data after veraison date')
    clim_window = ds_clim_sub.sel(time = veraison_range, method = 'nearest') #method = 'nearest' needed, otherwise error due to NAN values

    logger.debug('Masking clim_window')
    clim_window = clim_window.where(mask_veraison) 
    
    logger.debug('Calculating indices')
    _clim_idx = xr.Dataset({
        'gdd': (clim_window.tas - 10).clip(min = 0).sum('nr'),
        'gdd_opt': (clim_window.tas - 25).clip(min = 0).sum('nr'),
        'pr_sum': (clim_window.pr).sum('nr'),
        'pr_max': (clim_window.pr).max('nr'),

        'days_max': (clim_window.tasmax > 40).sum('nr'),
        'days_min': (clim_window.tasmin < 10).sum('nr'),
        'tasmin': (clim_window.tasmin).mean('nr'),
        'tasmax': (clim_window.tasmax).mean('nr')
    })
    _clim_idx = _clim_idx.assign_coords({'Prime': v_name})

    clim_idx.append(_clim_idx)

clim_idx = xr.concat(clim_idx, dim = 'Prime')