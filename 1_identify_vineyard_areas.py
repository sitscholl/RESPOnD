import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
from shapely import Polygon
import rasterio
from rasterio.features import rasterize
import rioxarray

from functions import config

##Load data
minx, miny, maxx, maxy = config.aois["europe"]
dem = xr.open_dataset(config.dem_chelsa).sel(lat = slice(miny, maxy), lon = slice(minx, maxx))
dem = dem.rio.write_crs(4326)

print('Dem loaded')

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

print('Fishnet created')

##Prepare vineyards
vineyards_luisa = gpd.read_file('data/vineyards/luisa_vineyards.shp').to_crs(4326).cx[minx:maxx,miny:maxy]
vineyards_osm = gpd.read_file('data/vineyards/osm_vineyards.shp').to_crs(4326).cx[minx:maxx,miny:maxy]
vineyards_shp = gpd.GeoDataFrame({'id': [1], 'geometry': [pd.concat([vineyards_luisa, vineyards_osm]).geometry.union_all()]}, crs = 4326)

gisco_lau = gpd.read_file(config.gisco_lau)[['GISCO_ID', 'geometry']].cx[minx:maxx,miny:maxy]
vineyards_lau = gpd.overlay(
    vineyards_shp, gisco_lau, how="intersection", keep_geom_type=True
)[["GISCO_ID", "geometry"]].dissolve(by="GISCO_ID").reset_index()

##Prepare PDOs
pdo_shp = gpd.read_file(config.downloader.fetch('EU_PDO.gpkg'))
gisco_points = gisco_lau.to_crs(pdo_shp.crs)
gisco_points['geometry'] = gisco_points.geometry.representative_point()
pdo_lau = gpd.sjoin(pdo_shp, gisco_points, how = 'inner', predicate = 'intersects')
gisco_pdo_link = pdo_lau.groupby('GISCO_ID', as_index = False).apply(lambda x: ';'.join(x['PDOid']), include_groups=False)
gisco_pdo_link.rename(columns = {None: 'PDOid'}, inplace = True)

##Intersect with vineyards
fishnet_inters = gpd.overlay(fishnet, vineyards_lau, how = 'intersection', keep_geom_type=True)
fishnet_inters['area'] = fishnet_inters.to_crs(3035).geometry.area

##For each grid, select the LAU with highest share of vineyard area
fishnet_inters = fishnet_inters.sort_values('area', ascending=False).drop_duplicates(['grid_id'], keep = 'first')

##Merge back to restore original fishnet geometry
fishnet_sub = fishnet.merge(fishnet_inters.drop('geometry', axis = 1), on = 'grid_id', how = 'inner')
fishnet_sub['area_share'] = fishnet_sub['area'] / fishnet_sub.to_crs(3035).geometry.area
fishnet_sub.drop(['area', 'grid_id'], axis = 1, inplace = True)
fishnet_sub['id'] = fishnet_sub.groupby('GISCO_ID').ngroup()

## Add PDOs (drops also polygons outside PDO areas)
fishnet_sub = fishnet_sub.merge(gisco_pdo_link, on = 'GISCO_ID', how = 'inner')

fishnet_sub.to_file(f'data/vineyards/vineyards_fishnet.shp')

print('Fishnet intersected')
# fishnet_sub = gpd.read_file(f'data/vineyards/vineyards_fishnet.shp')
##Rasterize
for col in ['id', 'area_share']:
    # create tuples of geometry, value pairs, where value is the attribute value you want to burn
    geom_value = [(geom,value) for geom, value in zip(fishnet_sub.geometry, fishnet_sub[col])]

    # Rasterize vector using the shape and transform of the raster
    rasterized = rasterize(
        geom_value,
        out_shape=dem.orog.shape,
        transform=dem.rio.transform(),
        all_touched=False,
        fill=0,
        dtype=np.float64,
    )

    profile = {
        'driver': 'GTiff',
        'dtype': np.float64,
        'nodata': 0,
        'width': dem.lon.size,
        'height': dem.lat.size,
        'count': 1,
        'crs': 4326,
        'transform': dem.rio.transform(),
        'tiled': True,
        'compress': 'lzw'
    }

    # Write output raster
    with rasterio.open(f'data/vineyards/rasterized_{col}.tif','w',**profile) as dst:
        dst.write(rasterized,1)

    print(f"Fishnet with parameter {col} rasterized.")
