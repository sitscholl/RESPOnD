import pandas as pd
import geopandas as gpd
import numpy as np
import warnings
import pooch
import requests
from bs4 import BeautifulSoup
from io import StringIO
from shapely import Polygon, MultiPolygon

from functions import config

def read_regtable(url):

    resp = requests.get(url)

    soup = BeautifulSoup(resp.content, 'html.parser')
    header = soup.find(lambda tag: tag.name == "h3" and "Sub Regions" in tag.text)
    if header is not None:
        html_subregions = header.find_next("table", id = 'subregions')
    else:
        html_subregions = soup.select('table', id='subregions')

    if html_subregions is not None:
        tbl_subregions = pd.read_html(StringIO(str(html_subregions)), extract_links = 'body')[0]
        tbl_subregions.columns = colnames

        tbl_subregions[['Region', 'URL']] = pd.DataFrame(tbl_subregions['Name'].tolist(), index = tbl_subregions.index)
        tbl_subregions['URL_shp'] = tbl_subregions['shp'].map(lambda x: x[-1])
        tbl_subregions.drop(['Name', 'OSM', 'size', 'shp'], axis = 1, inplace = True)
    else:
        tbl_subregions = None
    return(tbl_subregions)

def get_boundary(url):
    
    tsplit1 = str(requests.get(url).content).split('END')
    tsplit2 = [i.split('\\n') for i in tsplit1]

    geoms = []
    for a in tsplit2:
        clean_list = [i for i in a if i.startswith(' ')]

        if len(clean_list) > 0:
            clean_list = [np.array(i.lstrip(' ').split('  ')).astype(float) for i in clean_list]
            geoms.append(Polygon(clean_list))

    if len(geoms) > 1:
        return(MultiPolygon(geoms))
    else:
        return(geoms[0])

colnames = ['Name', 'OSM', 'size', 'shp']
main_page = r"https://download.geofabrik.de"
regions = ['Europe', 'Africa']

main_tbl = read_regtable(main_page)
main_tbl = main_tbl.loc[main_tbl['Region'].isin(regions)]
reg_tbl = main_tbl.copy()
reg_tbl['has_subregions'] = True
reg_tbl['geometry'] = None

while reg_tbl['has_subregions'].any():
    for url in reg_tbl.loc[reg_tbl['has_subregions'], 'URL']:
        tbl_subregions = read_regtable(f"{main_page}/{url}")

        if tbl_subregions is None:
            bndry = get_boundary(f"{main_page}/{url.replace('html', 'poly')}")
            reg_tbl.loc[reg_tbl['URL'] == url, ["has_subregions", 'geometry']] = False, bndry
        else:
            reg_tbl.drop(reg_tbl.loc[reg_tbl['URL'] == url].index, inplace = True)
            tbl_subregions['has_subregions'] = True

            base_url = "/".join(url.split('/')[:-1])
            for c in ['URL', 'URL_shp']:
                tbl_subregions[c] = tbl_subregions[c].map(lambda x: f'{base_url}/{x}' if x is not None else x)

            reg_tbl = pd.concat([reg_tbl, tbl_subregions]).reset_index(drop = True)

osm_regions = gpd.GeoDataFrame(data = reg_tbl, geometry = 'geometry', crs = 4326)

eu_pdo = gpd.read_file(config.downloader.fetch('EU_PDO.gpkg')).to_crs(osm_regions.crs)
osm_sub = gpd.sjoin(osm_regions, eu_pdo, predicate = 'intersects', how = 'inner')[osm_regions.columns].drop_duplicates(subset = 'Region')

vin_shp = []
for url in osm_sub['URL_shp']:
    try:
        _shp = gpd.read_file(f"{main_page}/{url}", layer = 'gis_osm_landuse_a_free_1').query('fclass == "vineyard"')
        vin_shp.append(_shp)
    except Exception as e:
        warnings.warn(f"Download failed for {url} with error:\n{e}")
vin_shp = pd.concat(vin_shp)

vin_shp[['osm_id', 'geometry']].to_file('data/vineyards/osm_vineyards.shp')



# luisa = pooch.retrieve(url="https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/LUISA/EUROPE/Basemaps/LandUse/2018/LATEST/LUISA_basemap_020321_50m.tif", 
#                        known_hash="5b82265265f2d6d1c474dac3f33fe62fd5be8e809676561b1bb64a96e821008f")

# arr_luisa = xr.open_dataset(luisa, chunks={'lat': 1000, 'lon': 1000})
# luisa_vineyards = arr_luisa.where(arr_luisa == 2210).compute()
