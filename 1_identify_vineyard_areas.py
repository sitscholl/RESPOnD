import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
import xagg as xa
import time
import pooch

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

clim_arr = xr.open_dataset('envelopes/clim_idx_2000_2004.nc').isel(year = 0)

##Create weightmap
start = time.time()
weightmap = xa.pixel_overlaps(clim_arr, pdo_vineyards)
end = time.time()

print(f"Time required to calcualte weightmap: {(end - start)/60:.1f} minutes")

##Save weightmap and vineyard shp
weightmap.to_file('prepared_data/wm')
# weightmap = xa.read_wm('prepared_data/wm')