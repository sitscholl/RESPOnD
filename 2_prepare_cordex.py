import os
import sys
import pandas as pd
import requests
import urllib3
import datetime

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
# bbox = {'north': 14.864502, 'south': 8.360596, 'west': 45.790509, 'east': 47.561701}
search_args = {
    'project': 'CORDEX-Adjust',
    'domain': 'EUR-11',
    'time_frequency': 'day',
    'from_timestamp': "1980-01-01T00:00:00Z",
    'to_timestamp': "2100-12-31T23:59:00Z",
    'facets': 'driving_model,rcm_name,bias_adjustment,ensemble',
    'latest': True,
}

chains = {
    'ICHEC-EC-EARTH': [('RACMO22E', 'r1i1p1'), ('HIRHAM5', 'r3i1p1'), ('CCLM4-8-17', 'r12i1p1'), ('RCA4', 'r12i1p1')],
    'MOHC-HadGEM2-ES': [('CCLM4-8-17', 'r1i1p1'), ('RACMO22E', 'r1i1p1'), ('RCA4', 'r1i1p1')],
    'MPI-M-MPI-ESM-LR': [('CCLM4-8-17', 'r1i1p1'), ('REMO2009', 'r1i1p1'), ('RCA4', 'r1i1p1')],
    'IPSL-IPSL-CM5A-MR': [('RCA4', 'r1i1p1')],
    'NCC-NorESM1-M': [('RCA4', 'r1i1p1')],
    #'CNRM-CERFACS-CNRM-CM5': []
}
variables = ['tasAdjust', 'prAdjust']

conn = SearchConnection('https://esgf-data.dkrz.de/esg-search', distrib=True)
# ctx = conn.new_context(bias_adjustment = 'v1-LSCE-IPSL-CDFt-EOBS10-1971-2005', **search_args)

# ctx.facet_counts['driving_model']
# ctx.facet_counts['bias_adjustment']
# ctx.facet_counts['rcm_name']
# ctx.facet_counts['ensemble']

all_files = []
for gcm, rcms in chains.items():
    for rcm, ensemble in rcms:
        for var in variables:

            ctx = conn.new_context(
                driving_model=gcm,
                rcm_name=rcm,
                experiment="rcp45",
                variable=var,
                ensemble=ensemble,
                #bias_adjustment = 'v1-LSCE-IPSL-CDFt-EOBS10-1971-2005',
                **search_args
            )
            ds_hist = ctx.search()

            if len(ds_hist) > 1:
                #raise ValueError('More than 1 dataset found. Refine search!')
                print(f'More than 1 dataset found for {gcm}-{rcm}-{ensemble}-{var}')
                print(ctx.facet_counts['bias_adjustment'])
                continue
            elif len(ds_hist) == 0:
                print(f'No datasets found for {gcm}-{rcm}-{ensemble}-{var}')
                continue

            files = ds_hist[0].file_context().search()
            fnames = [i.filename for i in files]
            #urls = [i.opendap_url for i in files]

            all_files.append(fnames)
            print(f"{gcm}-{rcm}-{ensemble}-{var}")
            continue

            # xr.open_dataset(files[0].opendap_url)

            sys.exit()






test = xr.open_mfdataset(urls[0:2], preprocess = lambda x: x.sel(rlat = slice(-5, 0), rlon = slice(-10, -5)))
test.tasAdjust.isel(time = 0).plot()
test.to_netcdf('test.nc')

test = xr.open_dataset(r"C:\Users\tscho\Downloads\tas_EUR-11_ICHEC-EC-EARTH_historical_r12i1p1_CLMcom-CCLM4-8-17_v1_day_19491201-19501231.nc")
test.sel(rlat = slice(-5, 0), rlon = slice(-10, -5)).tas.isel(time = 0).plot()

test = results[1].file_context()#.search()

test_df = pd.DataFrame(list(
    map(
        lambda f: {
            "filename": f.filename,
            "url": f.download_url,
            "opendap": f.opendap_url,
        },
        test,
    )
))
test_df['Period'] = test_df['filename'].str.split('_', expand = True).iloc[:,-1]
test_df['url'][0]
