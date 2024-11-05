import pandas as pd
# import geopandas as gpd
import numpy as np
import xarray as xr
# from rasterio.enums import Resampling
import matplotlib.pyplot as plt
from itertools import product

tbl_parker = pd.read_csv('prepared_data/parker_2013.csv').drop('Unnamed: 0', axis = 1).dropna(subset = 'Prime Name')
tbl_candiago = pd.read_csv('prepared_data/candiago_2022.csv').drop('Unnamed: 0', axis = 1)
# candiago_shp = gpd.read_file('prepared_data/EU_PDO.gpkg')
parker_sub = (
    tbl_parker[["Prime Name", "F*"]]
    .copy()
    .loc[tbl_parker["Prime Name"].isin(tbl_candiago["Prime Name"].unique())]
    .dropna(subset="Prime Name")
)

aoi_lat = slice(46, 47) #slice(44, 48)
aoi_lon = slice(10, 12) #slice(4, 16)
resolution = "90arcsec"
years = np.arange(2000, 2003)
months = np.arange(3, 10)
variables = ['tas', 'tasmin', 'tasmax', 'pr']
veraison_max = 275

url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
urls = []
for var in variables:
    urls.extend(
        [url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
         for y,m in product(years, months)
        ]
    )
ds_clim = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=aoi_lat, lon=aoi_lon)

##Calculate temperature cumsum for each year after 60th doy
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

clim_idx = []
for v_name, Fcrit in list(zip(parker_sub['Prime Name'], parker_sub['F*'])):

    ##Drop days where all pixels are below Fcrit
    tas_sub = tas_sum.where(tas_sum >= Fcrit)
    times_sel = ~tas_sub.isnull().all(['lat', 'lon'])
    del tas_sub

    ##Subtract Fcrit
    tas_diff = tas_sum.sel(time = times_sel) - Fcrit
    tas_diff = tas_diff.where(tas_diff >= 0)

    ##Get date when Fcrit is reached for each year
    veraison_date = tas_diff.groupby('time.year').apply(lambda c: c.idxmin(dim="time"))

    ##Find dates that are prior to veraison_max
    veraison_date = veraison_date.where(veraison_date.dt.dayofyear < veraison_max)

    ##Create a mask containing all pixels that have a veraison date
    mask_veraison = veraison_date.groupby('year').apply(lambda x: x.notnull().any('year'))

    ##Expand array to add 45 days after Fcrit is reached
    # test4 = test3.expand_dims(nr = 1).interp(nr = np.arange(45)+1)
    veraison_range = xr.concat(
        [
            (veraison_date + np.timedelta64(i, "D")).assign_coords({"nr": i})
            for i in np.arange(45) + 1
        ],
        dim="nr",
    )

    ##Remove dates that 'jumped to' next year (when using max. threshold for veraison_date, this should not be needed anymore??)
    # Fcrit_range = Fcrit_range.where(Fcrit_range.dt.month > 3)

    ##Get climatic variables within 45 day window for valid pixels
    clim_window = ds_clim.sel(time = veraison_range, method = 'nearest').where(mask_veraison) #method = 'nearest' needed, otherwise error due to NAN values
    
    for v in ['tas', 'tasmin', 'tasmax']:
        clim_window[v] = clim_window[v] - 273.5

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