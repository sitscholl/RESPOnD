import logging
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import datetime

#####Testing
# from itertools import product

# aoi_lat = slice(27, 57)
# aoi_lon = slice(-19.4, 34.5)
# url_template = "https://files.isimip.org/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5/chelsa-w5e5_obsclim_{variable}_{resolution}_global_daily_{timestamp}.nc#mode=bytes"
# urls = []
# for var in ['tas']:
#     urls.extend(
#         [url_template.format(variable=var, resolution="1800arcsec", timestamp=f"{y}{m:02}")
#          for y,m in product([2000], np.arange(1, 13))
#         ]
#     )
# ds = xr.open_mfdataset(urls, chunks="auto", join = 'override').sel(lat=aoi_lat, lon=aoi_lon)
# if len(ds.chunks) > 0:
#     ds = ds.compute()
# Fcrit = 2500
# veraison_min=214
# veraison_max=275
# veraison_window=45

# ds['tas'] = ds['tas'] - 273.5
#####

def calc_phen_date(tas, Fcrit):
    logger = logging.getLogger('main')

    ##Check if necessary dims are present
    if not 'time' in tas.dims:
        raise ValueError('Time dimension not found in dataset.')

    ##Check if unit is °C
    if (tas.max() > 100).any().item():
        raise ValueError('Temperature values above 100 detected. Make sure units are in °C')

    ##Calculate temperature cumsum for each year after 60th doy
    logger.debug('Calculating temperature cumulative sum')
    tas_sum = (
        (tas.sel(time=(tas.time.dt.dayofyear >= 60)))
        .clip(min=0)
        .groupby("time.year")
        .cumsum()
    )

    ##Drop days where all pixels are below Fcrit
    logger.debug('Droping days below Fcrit')
    tas_sub = tas_sum.where(tas_sum >= Fcrit)
    times_sel = ~tas_sub.isnull().all(['lat', 'lon'])
    del tas_sub

    ##Subtract Fcrit
    logger.debug('Calculating temperature difference')
    tas_diff = tas_sum.sel(time = times_sel) - Fcrit
    tas_diff = tas_diff.where(tas_diff >= 0)

    ##Get date when Fcrit is reached for each year
    logger.debug('Getting veraison date for each year')
    phen_date = tas_diff.groupby('time.year').apply(lambda c: c.idxmin(dim="time"))

    return(phen_date)

def get_climatic_window(ds, phen_date, window):
    logger = logging.getLogger('main')

    ##Check if necessary dims are present
    if not 'time' in ds.dims:
        raise ValueError('Time dimension not found in dataset.')

    ##Create a mask containing all pixels that have a veraison date
    logger.debug('Creating valid mask')
    mask_veraison = phen_date.groupby('year').apply(lambda x: x.notnull().any('year'))

    ##Expand array to add 45 days after Fcrit is reached
    logger.debug('Expanding array')
    veraison_range = xr.concat(
        [
            (phen_date + np.timedelta64(i, "D")).assign_coords({"nr": i})
            for i in np.arange(window)
        ],
        dim="nr",
    )

    ##Remove dates that 'jumped to' next year (when using max. threshold for phen_date, this should not be needed anymore??)
    # Fcrit_range = Fcrit_range.where(Fcrit_range.dt.month > 3)

    ##Check if required dates are present
    if ds.time.min().item() > veraison_range.min(skipna = True).item():
        raise ValueError(f'Timerange in ds too small! Make sure to include additional months at start of year to cover entire veraison_range. ds starts at {ds.time.min().values} and veraison_range starts at {veraison_range.min(skipna = True).values}')

    ##Get climatic variables within 45 day window for valid pixels
    logger.debug('Extracting climatic data after veraison date')
    clim_window = ds.sel(time = veraison_range, method = 'nearest') #method = 'nearest' needed, otherwise error due to NAN values

    logger.debug('Masking clim_window')
    clim_window = clim_window.where(mask_veraison) 

    return(clim_window)
