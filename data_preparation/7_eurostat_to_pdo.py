import pandas as pd
from pathlib import Path
import numpy as np
import geopandas as gpd
import urllib.request

from functions import config

pdo_shp = gpd.read_file(config.downloader.fetch('EU_PDO.gpkg'))
pdo_varieties = pd.read_csv('data/candiago_2022.csv')

eurostat = gpd.read_file('data/eurostat_2020.shp').to_crs(pdo_shp.crs)
eurostat['Area'] = eurostat['Area 2020'].fillna(eurostat['Area 2015'])
eurostat_shp = eurostat[["NUTS_ID", "geometry"]].drop_duplicates()

varieties = np.sort(eurostat['Prime Name'].dropna().unique())

vineyards_luisa = gpd.read_file('data/vineyards/luisa_vineyards.shp').to_crs(pdo_shp.crs)
vineyards_osm = gpd.read_file('data/vineyards/osm_vineyards.shp').to_crs(pdo_shp.crs)
vineyards_shp = gpd.GeoDataFrame({'id': [1], 'geometry': [pd.concat([vineyards_luisa, vineyards_osm]).geometry.union_all()]}, crs = pdo_shp.crs)

##Intersect PDOs and vineyards
pdo_vineyards = (
    gpd.overlay(pdo_shp, vineyards_shp, how="intersection", keep_geom_type=True)
    .explode()
)

del vineyards_shp

pdo_vineyards = (
    pdo_vineyards.loc[pdo_vineyards.geometry.area >= 10000]
    .copy()
    .dissolve(by="PDOid")
    .reset_index()
)
print('Intersected vineyards')

pdo_areas = []
for var_name in varieties:

    print(f'Processing {var_name}')

    ##Select pdos and nuts that contain variety
    pdos_var = pdo_varieties.loc[pdo_varieties['Prime Name'] == var_name, 'PDOid'].unique()
    nuts_var = eurostat.loc[eurostat['Prime Name'] == var_name, 'NUTS_ID'].unique()

    ##Intersect PDO vineyards and Eurostat
    pdo_vineyards_nuts = gpd.overlay(
        pdo_vineyards.loc[pdo_vineyards["PDOid"].isin(pdos_var)],
        eurostat_shp.loc[eurostat_shp["NUTS_ID"].isin(nuts_var)],
        how="intersection",
        keep_geom_type=True,
    )

    if pdo_vineyards_nuts.shape[0] > 0:

        pdo_vineyards_nuts = (
            pdo_vineyards_nuts.loc[pdo_vineyards_nuts.geometry.area >= 100000]
            .copy()
        )
        pdo_vineyards_nuts['Vin_Area_PDO'] = pdo_vineyards_nuts.geometry.area
        pdo_vineyards_nuts['Vin_Area_NUTS'] = pdo_vineyards_nuts.groupby('NUTS_ID')['Vin_Area_PDO'].transform("sum")
        pdo_vineyards_nuts = pdo_vineyards_nuts.merge(
            eurostat.loc[eurostat["Prime Name"] == var_name, ["NUTS_ID", "Area"]],
            on="NUTS_ID",
            how="left",
        )

        pdo_vineyards_nuts['Cultivation Area'] = pdo_vineyards_nuts['Area'] * (pdo_vineyards_nuts['Vin_Area_PDO'] / pdo_vineyards_nuts['Vin_Area_NUTS'])
        
        pdo_cult_area = pdo_vineyards_nuts.groupby('PDOid', as_index = False)['Cultivation Area'].sum()
        pdo_cult_area['Prime Name'] = var_name

        pdo_areas.append(pdo_cult_area)

pdo_areas = pd.concat(pdo_areas)
pdo_areas_merge = pdo_varieties.merge(pdo_areas, on = ['PDOid', 'Prime Name'], how = 'left')

pdo_areas_merge.to_csv('data/candiago_2022_area.csv')