import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from pathlib import Path
import argparse
import pydist
import logging
import logging.config

logging.config.fileConfig(".config/logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

##Arguments
parser = argparse.ArgumentParser()
parser.add_argument('out_dir', help = 'Output directory where results will be stored')
# parser.add_argument('-v', '--variables', default = ['tas'], nargs='+', help = 'Variables to download. Choose one or more of tas, tasmax, tasmin and pr')
parser.add_argument('-a', '--aoi', default = 'europe', help = 'Name of area of interest for analysis.')
parser.add_argument('-r', '--resolution', default = 1800, type = int, help = 'Resolution of climate grids in arcseconds.')
parser.add_argument('-ys', '--year_start', default = 2000, type = int, help = 'Starting year of climate grids.')
parser.add_argument('-ye', '--year_end', default = 2001, type = int, help = 'Last year of climate grids. Must be greater than year_start.')
parser.add_argument('-yc', '--year_chunks', default = 1, type = int, help = 'Number of years that should be processed at once. Depends on RAM of host. Default is to process each year individually.')

args = parser.parse_args()

if args.aoi not in list(pydist.config.aois.keys()):
    raise ValueError(f"Invalid input for aoi argument. Choose one of {', '.join(list(pydist.config.aois.keys()))}")
minx, miny, maxx, maxy = pydist.config.aois[args.aoi]

if args.resolution not in [30, 90, 300, 1800]:
    raise ValueError(f"Invalid input for resolution argument. Choose one of: 30, 90, 300, 1800")
resolution = f"{args.resolution}arcsec"

if args.year_start > args.year_end:
    raise ValueError('year_start must be greater than year_end!')
if any([args.year_start < 1979, args.year_end < 1979, args.year_start > 2016, args.year_end > 2016]):
    raise ValueError('year_start and year_end arguments must both fall within 1979-2016. Values outside this range are not supported.')
years = np.arange(args.year_start, args.year_end+1)

if args.year_chunks < 1:
    raise ValueError('year_chunks must at least be 1, smaller values are not allowed.')
y_chunks = args.year_chunks

out_dir = Path(args.out_dir, resolution)
out_dir.mkdir(exist_ok=True, parents=True)
out_phen = Path(out_dir, 'veraison_dates')
out_phen.mkdir(exist_ok=True, parents=True)
out_csv = Path(out_dir, 'climatic_indices.csv')

# Fixed arguments
veraison_min, veraison_max = 214, 275
clim_window_length = 45
months = np.arange(3, 13)
variables = ['tas', 'tasmax', 'tasmin', 'pr']

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

    #TODO: Check which years of y_group are already present in climatic_indices table and use only those

    ##Load chelsa data
    logger.info('Loading climate data')
    ds = pydist.load_chelsa_w5e5(variables, resolution, y_group, months = months, aoi = (minx, miny, maxx, maxy))

    ##Align weight and climate arrays
    sys.exit()
    vn_arr_re, vn_weights_re = pydist.align_arrays(vn_arr, vn_weights, base = ds.isel(time = 0).tas)

    ##Iterate over varieties
    for v_name, Fcrit in list(zip(parker_sub['Prime Name'], parker_sub['F*'])):

        logger.info(f'Processing variety {parker_sub["Prime Name"].tolist().index(v_name)+1}/{len(parker_sub["Prime Name"])}: {v_name}!')

        ##Calculate array with veraison dates
        veraison_date = pydist.calc_phen_date(ds.tas, Fcrit)
        pydist.save_array(veraison_date.dt.dayofyear, Path(f'{out_phen}/{v_name}.nc'), unlimited_dim = 'year')

        ##Find dates that are within veraison_min and veraison_max
        logger.debug('Masking veraison date')
        veraison_date = veraison_date.where((veraison_date.dt.dayofyear < veraison_max) & (veraison_date.dt.dayofyear >= veraison_min))

        clim_window = pydist.get_climatic_window(ds, veraison_date, window = clim_window_length)

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

        file_exists = out_csv.exists()
        _clim_pdo.to_csv(out_csv, header=not file_exists, mode='a' if file_exists else 'w')

