import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
from shapely import Polygon
from rasterio.features import rasterize
import rioxarray

from functions import config

##Load data
dem_chelsa = r"https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_orog_30arcsec_global.nc#mode=bytes"
minx, miny, maxx, maxy = config.aois['europe']
dem = xr.open_dataset(dem_chelsa).sel(lat = slice(miny, maxy), lon = slice(minx, maxx))
dem = dem.rio.write_crs(4326)

##Create fishnet grid
vals, counts = np.unique(np.diff(dem.lat.values), return_counts = True)
fishnet_res = vals[np.argmax(counts)]
geoms = []
for y in dem.lat:
    for x in dem.lon:
        x_c, y_c = x-(fishnet_res/2), y-(fishnet_res/2)
        pl = Polygon([(x_c,y_c), (x_c+fishnet_res,y_c), (x_c+fishnet_res, y_c+fishnet_res), (x_c, y_c+fishnet_res)])
        geoms.append(pl)

fishnet = gpd.GeoDataFrame(data = {'grid_id': np.arange(len(geoms))}, geometry = geoms, crs = 4326) 

##Intersect with vineyard landcover
vineyards_shp = gpd.read_file('../data/vineyards_europe_lau.shp').to_crs(4326).cx[minx:maxx,miny:maxy]

fishnet_inters = gpd.overlay(fishnet, vineyards_shp, how = 'intersection', keep_geom_type=True)
fishnet_inters['area'] = fishnet_inters.to_crs(3035).geometry.area
fishnet_inters = fishnet_inters.sort_values('area', ascending=False).drop_duplicates(['grid_id'], keep = 'first')

##For each grid, select LAUid with highest share of vineyard area
fishnet_sub = fishnet.merge(fishnet_inters.drop('geometry', axis = 1), on = 'grid_id', how = 'inner')
fishnet_sub['area_share'] = fishnet_sub['area'] / fishnet_sub.to_crs(3035).geometry.area
fishnet_sub.drop(['area', 'grid_id'], axis = 1, inplace = True)
fishnet_sub['id'] = np.arange(len(fishnet_sub))+1

fishnet_sub[['id', 'LAUid']].to_csv('prepared_data/vineyards/_fishnet_lau_map.csv', index = False)
##Rasterize
for thresh in [0, 0.03, 0.05]:
    fishnet_thresh = fishnet_sub.loc[fishnet_sub['area_share'] >= thresh]

    # create tuples of geometry, value pairs, where value is the attribute value you want to burn
    geom_value = [(geom,value) for geom, value in zip(fishnet_thresh.geometry, fishnet_thresh['id'])]

    # Rasterize vector using the shape and transform of the raster
    rasterized = rasterize(
        geom_value,
        out_shape=dem.orog.shape,
        transform=dem.rio.transform(),
        all_touched=False,
        fill=0,
        dtype=np.int16,
    )
    rasterized_arr = dem.orog.copy()
    rasterized_arr[:] = rasterized

    rasterized_arr.rio.to_raster(f'prepared_data/vineyards/rasterized_{thresh}.tif')
    fishnet_thresh.to_file(f'prepared_data/vineyards/fishnet_{thresh}.shp')