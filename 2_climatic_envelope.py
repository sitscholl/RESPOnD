import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from itertools import product
import logging
import xagg as xa
import pooch
from functions.get_climatic_window import get_climatic_window

# create logger
logger = logging.getLogger('main')
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
parker_sub = (
    tbl_parker[["Prime Name", "F*"]]
    .copy()
    .loc[tbl_parker["Prime Name"].isin(tbl_candiago["Prime Name"].unique())]
    .dropna(subset="Prime Name")
)

##Load vineyard landcover
vineyards_shp = gpd.read_file('../data/vineyards_europe_lau.shp')#.cx[4309705.6318:4611308.8673,2528263.1823:2669324.8642]
vineyards_shp = vineyards_shp.to_crs(4326)
pdo_path = pooch.retrieve(
    "https://springernature.figshare.com/ndownloader/files/35955185",
    fname="EU_PDO.gpkg",
    known_hash="8df0dc759f9c1bc17fff12d653b224282382d6a10f4a15c2cf937d5ab13f0356",
)
pdo_shp = gpd.read_file(pdo_path).to_crs(vineyards_shp.crs)

##Intersect PDOs and vineyards
pdo_vineyards = (
    gpd.overlay(pdo_shp, vineyards_shp, how="intersection", keep_geom_type=True)
)
pdo_vineyards = pdo_vineyards.loc[pdo_vineyards.to_crs(3035).geometry.area >= 10000]
pdo_vineyards.sindex

##Parameters for climatic data
aoi_lat = slice(27, 57) #slice(46, 47) #slice(44, 48)
aoi_lon = slice(-19.4, 34.5) #slice(10, 12) #slice(4, 16)
resolution = "1800arcsec"
years = np.arange(2000, 2003)
months = np.arange(3, 12)
variables = ['tas', 'tasmin', 'tasmax', 'pr']

url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
urls = []
for var in variables:
    urls.extend(
        [url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
         for y,m in product(years, months)
        ]
    )
logger.debug('Loading data into Dataset')
ds = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=aoi_lat, lon=aoi_lon)

if len(ds.chunks) > 0:
    ds = ds.compute()
for var in ds.keys():
    if 'tas' in var:
        ds[var] = ds[var] - 273.5

clim_idx = []
for v_name, Fcrit in list(zip(parker_sub['Prime Name'], parker_sub['F*']))[0:1]:

    logger.info(f'Starting variety {v_name}!')

    clim_window = get_climatic_window(ds, Fcrit, save_veraison_plots = True, plot_name = v_name)
    
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

    weightmap = xa.pixel_overlaps(_clim_idx, pdo_vineyards)
    clim_agg = xa.aggregate(_clim_idx, weightmap)
    clim_agg_df = clim_agg.to_dataframe()
    clim_agg_df = clim_agg_df.reset_index().groupby(['PDOid', 'year'])[clim_agg_df.select_dtypes(np.number).columns].mean()

    clim_idx.append(clim_agg_df)

# clim_idx = xr.concat(clim_idx, dim = 'Prime')

#clim_idx.to_netcdf('envelopes/clim_idx_2000_2004.nc')
