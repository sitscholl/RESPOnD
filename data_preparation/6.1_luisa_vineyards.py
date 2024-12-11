import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
import rioxarray
from shapely import Polygon
from rasterio.features import shapes

from functions import config

arr_luisa = xr.open_dataset(config.downloader.fetch("LUISA_50m.tif"), chunks={'y': 7000, 'x': 7000}).band_data.squeeze(drop = True)

iy, ix = arr_luisa.chunks
idx = [sum(ix[:i]) for i in range(len(ix)+1)]
idy = [sum(iy[:i]) for i in range(len(iy)+1)]

l_vins = []
for a,_ in enumerate(idx):
    print(a)
    for b,_ in enumerate(idy):
        if (a < len(idx)-1) and (b < len(idy)-1):

            tgt_x = xr.DataArray(np.arange(idx[a], idx[a+1]), dims="x")
            tgt_y = xr.DataArray(np.arange(idy[b], idy[b+1]), dims="y")

            da = arr_luisa.isel(y=tgt_y, x=tgt_x).compute()

            if da.isnull().all().item():
                continue

            vineyards_chunks = da.where(da == 2210).fillna(0).astype(np.int16)

            if (vineyards_chunks == 0).all().item():
                continue

            geoms = list(shapes(vineyards_chunks.values, mask = vineyards_chunks.values != 0, transform = vineyards_chunks.rio.transform()))
            for i in geoms:
                coords = i[0]['coordinates']
                if len(coords) > 1:
                    ply = Polygon(coords[0], holes = coords[1:])
                else:
                    ply = Polygon(coords[0])
                l_vins.append(ply)

gdf_vins = gpd.GeoDataFrame({'id': np.arange(len(l_vins)), 'geometry': l_vins}, crs = 3035)
gdf_vins.to_file('data/vineyards/luisa_vineyards.shp')