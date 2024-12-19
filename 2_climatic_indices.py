import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from itertools import product
import logging
from pathlib import Path
import argparse

from functions import config
from functions.get_climatic_window import calc_phen_date, get_climatic_window
from functions.save_array import save_array

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

##Arguments
parser = argparse.ArgumentParser()
# parser.add_argument('-v', '--variables', default = ['tas'], nargs='+', help = 'Variables to download. Choose one or more of tas, tasmax, tasmin and pr')
parser.add_argument('-a', '--aoi', default = 'europe', help = 'Name of area of interest for analysis.')
parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')
parser.add_argument('-ys', '--year_start', default = 2000, type = int, help = 'Starting year of climate grids.')
parser.add_argument('-ye', '--year_end', default = 2001, type = int, help = 'Last year of climate grids. Must be greater than year_start.')
parser.add_argument('-yc', '--year_chunks', default = 1, type = int, help = 'Number of years that should be processed at once. Depends on RAM of host. Default is to process each year individually.')

args = parser.parse_args()

if args.aoi not in list(config.aois.keys()):
    raise ValueError(f"Invalid input for aoi argument. Choose one of {', '.join(list(config.aois.keys()))}")
minx, miny, maxx, maxy = config.aois[args.aoi]

if args.resolution not in [30, 90, 300, 1800]:
    raise ValueError(f"Invalid input for resolution argument. Choose one of: 30, 90, 300, 1800")
resolution = f"{args.resolution}arcsec"

if args.year_start >= args.year_end:
    raise ValueError('year_start must be greater than year_end!')
if any([args.year_start < 1979, args.year_end < 1979, args.year_start > 2016, args.year_end > 2016]):
    raise ValueError('year_start and year_end arguments must both fall within 1979-2016. Values outside this range are not supported.')
years = np.arange(args.year_start, args.year_end)

if args.year_chunks < 1:
    raise ValueError('year_chunks must at least be 1, smaller values are not allowed.')
y_chunks = args.year_chunks

# Fixed arguments
veraison_min, veraison_max = 214, 275
clim_window_length = 45
months = np.arange(3, 12)
variables = ['tas', 'tasmax', 'tasmin', 'pr']

##Logger
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logger.info(f'Starting script. Processing years {args.year_start} to {args.year_end} for aoi {args.aoi} at a resolution of {args.resolution}arcsec')

##Load phenological data
tbl_parker = pd.read_csv('data/parker_2013.csv').drop('Unnamed: 0', axis = 1).dropna(subset = 'Prime Name')
vin_area = pd.read_csv('data/candiago_2022_area.csv').drop(['Unnamed: 0.1', 'Unnamed: 0'], axis = 1).rename(columns = {'Prime Name': 'Prime'})

parker_sub = (
    tbl_parker[["Prime Name", "F*"]]
    .copy()
    .loc[tbl_parker["Prime Name"].isin(vin_area["Prime"].unique())]
    .dropna(subset="Prime Name")
)

##Load vineyard landcover and calculate weightmap
vn_fishnet = gpd.read_file('data/vineyards/vineyards_fishnet.shp').drop('geometry', axis = 1).drop_duplicates(subset = 'GISCO_ID')
vn_fishnet['PDOid'] = vn_fishnet['PDOid'].map(lambda x: x.split(';'))
vn_fishnet = vn_fishnet.explode('PDOid').sort_values(['PDOid', 'id'])

vn_arr = xr.open_dataset('data/vineyards/rasterized_id.tif').band_data.squeeze(drop = True).rename({'y': 'lat', 'x': 'lon'})
vn_arr.name = 'id'
# vn_arr = vn_arr.rio.set_nodata(0)
# vn_arr = vn_arr.fillna(0).astype(int)
vn_weights = xr.open_dataset('data/vineyards/rasterized_area_share.tif').band_data.squeeze(drop = True).rename({'y': 'lat', 'x': 'lon'})

# weightmap = vn_weights.groupby(vn_arr) / vn_weights.groupby(vn_arr).sum(skipna = True, min_count = 1)
# weightmap = weightmap.drop_vars('id')

clim_idx = []
vn_arr_re = vn_arr.copy()
vn_weights_re = vn_weights.copy()
# _clim_idx = xr.open_dataset('envelopes/clim_idx_2000.nc').isel(Prime = 0)
for y_group in chunker(years, y_chunks):
    logger.info(f"Processing year(s): {', '.join(y_group.astype(str))}")

    ##Generate list of urls
    urls = []
    for var in variables:
        urls.extend(
            [config.url_template.format(variable=var, resolution=resolution, timestamp=f"{y}{m:02}")
            for y,m in product(y_group, months)
            ]
        )

    ##Load data into memory
    logger.debug('Loading data into Dataset')
    ds = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=slice(miny, maxy), lon=slice(minx, maxx))

    if len(ds.chunks) > 0:
        ds = ds.compute()
    for var in ds.keys():
        if 'tas' in var:
            ds[var] = ds[var] - 273.5

    ##Align weight and climate arrays
    ds = ds.rio.write_crs(4326)
    if (vn_arr_re.lat.shape != ds.lat.shape) or (vn_arr_re.lon.shape != ds.lon.shape):

        logger.info('Reprojecting')
        tmpl = ds.isel(time = 0).tas

        vn_arr_re = (
            vn_arr.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
            .rio.reproject_match(tmpl)
            .rename({"x": "lon", "y": "lat"})
        )

        vn_weights_re = (
            vn_weights.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
            .rio.reproject_match(tmpl)
            .rename({"x": "lon", "y": "lat"})
        )

        # weightmap_re = weightmap.rio.set_spatial_dims(x_dim = 'lon', y_dim = 'lat').rio.reproject_match(tmpl).rename({'x': 'lon', 'y': 'lat'})

    for v_name, Fcrit in list(zip(parker_sub['Prime Name'], parker_sub['F*'])):

        logger.info(f'Processing variety {v_name}!')

        ##Calculate array with veraison dates
        veraison_date = calc_phen_date(ds.tas, Fcrit)
        save_array(veraison_date.dt.dayofyear, Path(f'data/results/veraison_dates/{v_name}.nc'), unlimited_dims = 'year')

        ##Find dates that are within veraison_min and veraison_max
        logger.debug('Masking veraison date')
        veraison_date = veraison_date.where((veraison_date.dt.dayofyear < veraison_max) & (veraison_date.dt.dayofyear >= veraison_min))

        clim_window = get_climatic_window(ds, veraison_date, window = clim_window_length)

        logger.debug('Calculating indices')
        _clim_idx = xr.Dataset({
            'gdd': (clim_window.tas - 10).clip(min = 0).sum('nr', skipna = True, min_count = 1),
            'gdd_opt': (clim_window.tas - 25).clip(min = 0).sum('nr', skipna = True, min_count = 1),
            'pr_sum': (clim_window.pr).sum('nr', skipna = True, min_count = 1),
            'pr_max': (clim_window.pr).max('nr'),

            'days_max': (clim_window.tasmax > 40).sum('nr', skipna = True, min_count = 1),
            'days_min': (clim_window.tasmin < 10).sum('nr', skipna = True, min_count = 1),
            'tasmin': (clim_window.tasmin).mean('nr'),
            'tasmax': (clim_window.tasmax).mean('nr')
        })
        _clim_idx = _clim_idx.assign_coords({'Prime': v_name})
        _clim_idx = _clim_idx.rio.write_crs(4326)

        ##Aggregate to LAU level
        logger.debug('Aggregating indices')
        _clim_agg = (
            (_clim_idx * vn_weights_re).groupby(vn_arr_re).sum(skipna=True, min_count=1)
        ) / vn_weights_re.groupby(vn_arr_re).sum(skipna=True, min_count=1)

        # _clim_agg = (_clim_idx * weightmap_re).groupby(vn_arr_re).sum(skipna = True, min_count = 1)

        _clim_agg = _clim_agg.to_dataframe().reset_index()
        _clim_agg['id'] = _clim_agg['id'].astype(int)
        # _clim_agg.dropna(subset = 'gdd', inplace = True)

        ##Aggregate to PDO level
        _clim_pdo = vn_fishnet[['id', 'PDOid']].merge(_clim_agg, on = 'id')
        _clim_pdo.drop(['id', 'spatial_ref'], axis = 1, inplace = True, errors = 'ignore')
        _clim_pdo = (
            _clim_pdo.groupby(["PDOid", "Prime", "year"], as_index=False)
            .mean(numeric_only=True)
            .merge(vin_area[["PDOid", "Prime"]], how="inner") #drops rows with varieties that are not authorized in a PDO
        )

        clim_idx.append(_clim_pdo)

tbl_idx = pd.concat(clim_idx)
tbl_idx.to_csv(f'data/results/climatic_indices/indices_{np.min(years)}_{np.max(years)}.csv')
