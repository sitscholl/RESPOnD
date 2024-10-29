import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.enums import Resampling
import matplotlib.pyplot as plt

tbl_parker = pd.read_csv('prepared_data/parker_2013.csv').drop('Unnamed: 0', axis = 1).dropna(subset = 'Prime Name')
tbl_candiago = pd.read_csv('prepared_data/candiago_2022.csv').drop('Unnamed: 0', axis = 1)
candiago_shp = gpd.read_file('prepared_data/EU_PDO.gpkg')

test_var = "PINOT NOIR"
aoi_lat = slice(46, 47) #slice(44, 48)
aoi_lon = slice(10, 12) #slice(4, 16)
resolution = "90arcsec"
variables = ['tas', 'pr']

Fcrit = xr.Dataset.from_dataframe(
    tbl_parker[["Prime Name", "F*"]]
    .loc[tbl_parker["Prime Name"].isin(tbl_candiago["Prime Name"].unique())]
    .dropna(subset = 'Prime Name')
    .rename(columns = {'Prime Name': 'Prime'})
    .set_index("Prime")
)['F*']

url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
urls = []
for var in variables:
    urls.extend(
        [url_template.format(variable=var, resolution=resolution, timestamp=f"2010{i:02}")
         for i in range(3, 10)
        ]
    )
ds_clim = xr.open_mfdataset(urls, chunks="auto").sel(lat=aoi_lat, lon=aoi_lon)

##Calculate temperature cumsum for each year after 60th doy
tas_sum = (
    (
        arr_tair.tas.sel(time=(arr_tair.time.dt.dayofyear >= 60))# & (ds.time.dt.year <= 1983))
        - 273.5
    )
    .clip(min=0)
    .groupby("time.year")
    .cumsum()
    .compute()
)

##Drop days where all pixels are below minimum Fcrit
tas_sub = tas_sum.where(tas_sum >= Fcrit.min().data)
times_sel = ~tas_sub.isnull().all(['lat', 'lon'])

##Subtract Fcrit per variety and create mask
tas_diff = tas_sum.sel(time = times_sel) - Fcrit
tas_diff = tas_diff.where(tas_diff >= 0)

##Get date when Fcrit is reached for each year
Fcrit_date = tas_diff.groupby('time.year').apply(lambda c: c.idxmin(dim="time"))

##Find dates that are prior to 1st Oct
Fcrit_date = Fcrit_date.where(Fcrit_date.dt.dayofyear < 275)
mask_Fcrit = Fcrit_date.groupby('year').apply(lambda x: x.notnull().any('year'))

##Expand array to add 45 days after Fcrit is reached
# test4 = test3.expand_dims(nr = 1).interp(nr = np.arange(45)+1)
Fcrit_range = xr.concat(
    [
        (Fcrit_date + np.timedelta64(i, "D")).assign_coords({"nr": i})
        for i in np.arange(45) + 1
    ],
    dim="nr",
)

##Remove dates that 'jumped to' next year (when using max. threshold for Fcrit_date, this should not be needed anymore??)
# Fcrit_range = Fcrit_range.where(Fcrit_range.dt.month > 3)
# plt.clf()
# Fcrit_range.isel(year = 0, nr = -1, Prime = 0).dt.dayofyear.plot()

##Get climatic variables within 45 day window for valid pixels
clim_window = arr_tair.sel(time = Fcrit_range, method = 'nearest').where(mask_Fcrit) #method = 'nearest' needed, otherwise error due to NAN values
clim_window['tas'] = clim_window.tas - 273.5

clim_idx = xr.Dataset({
    'gdd': (clim_window.tas - 10).clip(min = 0).sum('nr'),
    'gdd_opt': (clim_window.tas - 25).clip(min = 0).sum('nr')
})


####
####
# tas_diff.isel(time = 0, prime = 0).plot()
# arr_tair.isel(time = 3).tas.plot()
# test2 = test2.rio.write_crs(4326)
# test2 = test2.rio.reproject(3035)
# test2.chunks
# test2.rio.resolution()
