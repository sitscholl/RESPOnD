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

# Log into ESGF Portal
openid = "https://esgf.nci.org.au/esgf-idp/openid/sitscholl"
lm = LogonManager()
if not lm.is_logged_on():
    lm.logon_with_openid(openid=openid, bootstrap=True, password="JugugOmA0uvuwrOCR?za")

if not lm.is_logged_on():
    raise ValueError('Log on failed!')

# Execute Query
bbox = {'north': 14.864502, 'south': 8.360596, 'west': 45.790509, 'east': 47.561701}
search_dict = {
    'project': 'CORDEX-Adjust',
    'domain': 'EUR-11',
    'time_frequency': 'day',
    'variable': ['tasAdjust', 'prAdjust'],
    'from_timestamp': "1980-01-01T00:00:00Z",
    'to_timestamp': "2100-12-31T23:59:00Z",
    'facets': 'driving_model,rcm_name,bias_adjustment',
    'latest': True,
    #'bbox': bbox
}

# Todo: Further differentiate RCMs by init id (r1i1p1 etc.)
chains = {
    'ICHEC-EC-EARTH': ['RACMO22E', 'HIRHAM5', 'CCLM4-8-17', 'RCA4'],
    'MOHC-HadGEM2-ES': ['CCLM4-8-17', 'RACMO22E', 'RCA4'],
    'MPI-M-MPI-ESM-LR': ['CCLM4-8-17', 'REMO2009', 'RCA4'],
    'IPSL-IPSL-CM5A-MR': ['RCA4'],
    'NCC-NorESM1-M': ['RCA4'],
    #'CNRM-CERFACS-CNRM-CM5': []
}
vars = ['tasAdjust', 'prAdjust']

conn = SearchConnection('https://esgf-data.dkrz.de/esg-search', distrib=True)

ctx = conn.new_context(driving_model = list(chains.keys()), experiment = 'rcp45', **search_dict)
ctx.facet_counts['driving_model']
ctx.facet_counts['bias_adjustment']
#ctx.facet_counts['rcm_name']

results = ctx.search()
for dataset in results:
    info = dataset.json
    driving_model = info['driving_model'][0]
    rcm_name = info['rcm_name'][0]
    var = info['variable'][0]

    if rcm_name not in chains[driving_model]:
        continue

    files = dataset.file_context().search()
    fnames = [i.filename for i in files]
    urls = [i.opendap_url for i in files]

    ctx_sim = ctx = conn.new_context(
        driving_model=driving_model,
        rcm_name=rcm_name,
        variable=var,
        experiment="rcp45",
        **{i: j for i, j in search_dict.items() if i != "variable"}
    )
    results_sim = ctx_sim.search()

    if len(results_sim) > 1:
        raise ValueError('More than 1 dataset found. Refine search!')
    sys.exit()

    # xr.open_dataset(files[0].opendap_url)

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
