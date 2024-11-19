import os
import sys
import pandas as pd
import requests
import urllib3
import datetime
import warnings

import numpy as np
import xarray as xr
from rasterio.enums import Resampling

# Activate this on windows before importing pyesgf, otherwise import of logon manager throws error
os.environ['HOME'] = os.environ['USERPROFILE'] 
from pyesgf.search import SearchConnection
from pyesgf.logon import LogonManager
# import xesmf as xe

os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "on"

# Log into ESGF Portal
openid = "https://esgf.nci.org.au/esgf-idp/openid/sitscholl"
lm = LogonManager()
if not lm.is_logged_on():
    lm.logon_with_openid(openid=openid, bootstrap=True, password="JugugOmA0uvuwrOCR?za")

if not lm.is_logged_on():
    raise ValueError('Log on failed!')

# Execute Query
search_args = {
    'project': 'CORDEX-Adjust',
    'domain': 'EUR-11',
    'time_frequency': 'day',
    'from_timestamp': "1980-01-01T00:00:00Z",
    'to_timestamp': "2100-12-31T23:59:00Z",
    'ensemble': 'r1i1p1',
    'experiment': 'rcp45',
    "bias_adjustment": 'v1-LSCE-IPSL-CDFt-EOBS10-1971-2005',
    'facets': 'driving_model,rcm_name,variable',
    'latest': True,
}

variables = ['tasAdjust', 'prAdjust']

chains_overview = pd.DataFrame(columns = ['GCM', 'RCM', 'variable'])

conn = SearchConnection('https://esgf-data.dkrz.de/esg-search', distrib=True)
ctx = conn.new_context(**search_args)
gcms = list(ctx.facet_counts['driving_model'].keys())
for gcm in gcms:
    ctx_gcm = ctx.constrain(driving_model = gcm)
    rcms = list(ctx_gcm.facet_counts['rcm_name'].keys())

    for rcm in rcms:
        ctx_rcm = ctx_gcm.constrain(rcm_name = rcm)
        vars_list = list(ctx_rcm.facet_counts['variable'].keys())

        if not all([i in vars_list for i in variables]):
            warnings.warn(f"Not all variables present for {gcm}-{rcm}")
            continue

        ds = []
        for var in variables:
            ctx_var = ctx_rcm.constrain(variable = var)
            dataset = ctx_var.search()

            if len(dataset) != 1:
                warnings.warn(f'{len(dataset)} files found for {gcm}-{rcm}-{var}!')
                continue

            files = dataset[0].file_context().search()
            fnames = [i.filename for i in files]
            urls = [i.opendap_url for i in files]

            sdates = [datetime.datetime.strptime(i.split('_')[-1].split('-')[0], '%Y%m%d') for i in fnames]
            urls_sel = [url for i,url in enumerate(urls) if sdates[i] >= datetime.datetime(1980, 1, 1)]

            ds_var = xr.open_mfdataset(urls_sel)[var]
            ds_var = ds_var.sel(rlat = slice(-5, 0), rlon = slice(-10, -5))

            ds.append(ds_var)

            #chains_overview.loc[len(chains_overview)] = (gcm, rcm, var)

        if len(set([i.shape for i in ds])) > 1:
            raise ValueError(f'Shape mismatch between variables for {gcm}-{rcm}!')
        ds = xr.merge(ds)


chains_overview['bool'] = 1
chains_overview.pivot(index = ['GCM', 'RCM'], columns='variable')


Fcrit = 2286

##Calculate temperature cumsum for each year after 60th doy
tas_sum = (
    (
        ds.tasAdjust.sel(time=(ds.time.dt.dayofyear >= 60))# & (ds.time.dt.year <= 1983))
        - 273.5
    )
    .clip(min=0)
    .groupby("time.year")
    .cumsum()
    .compute()
)

##Subtract Fcrit per variety and create mask
tas_diff = tas_sum - Fcrit
tas_diff = tas_diff.where(tas_diff >= 0)
mask_diff = tas_diff.groupby('time.year').apply(lambda x: x.notnull().any('time')) #Finds pixels that reach Fcrit during the year

##Get date when Fcrit is reached for each year
Fcrit_date = tas_diff.groupby('time.year').apply(lambda c: c.idxmin(dim="time"))

##Expand array to add 45 days after Fcrit is reached
# test4 = test3.expand_dims(nr = 1).interp(nr = np.arange(45)+1)
Fcrit_range = xr.concat(
    [
        (Fcrit_date + np.timedelta64(i, "D")).assign_coords({"nr": i})
        for i in np.arange(45) + 1
    ],
    dim="nr",
)

##Remove dates that 'jumped to' next year
Fcrit_range = Fcrit_range.where(Fcrit_range.dt.month > 3)
plt.clf()
Fcrit_range.isel(year = 0, nr = -1).dt.dayofyear.plot()

##Get climatic variables within 45 day window for valid pixels
clim_window = ds.sel(time = Fcrit_range, method = 'nearest').where(mask_diff) #method = 'nearest' needed, otherwise error due to NAN values
plt.clf()
clim_window.tasAdjust.isel(year = 0, nr = -1).plot()


# test = xr.open_dataset(
#     #r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_orog_30arcsec_global.nc"
#     #r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_pr_1800arcsec_global_daily_198711.nc"
#     #r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_tas_90arcsec_global_daily_197901.nc"
#     #r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_rsds_90arcsec_global_daily_197901.nc"
#     r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_pr_30arcsec_global_daily_197901.nc"
#     + "#mode=bytes",
#     chunks = 'auto'
# )
# test.chunks
# test2 = test.sel(lat = slice(43, 47), lon = slice(5, 14))
# test2 = test2.rio.write_crs(4326)
# test2 = test2.rio.reproject(3035)
# test2.chunks
# test2.rio.resolution()

# #import matplotlib.pyplot as plt
# plt.clf()
# test2.pr.isel(time = 3).plot()
