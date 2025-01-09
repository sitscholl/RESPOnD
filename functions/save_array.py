import netCDF4
import xarray as xr
from pathlib import Path
import datetime
import warnings
import numpy as np

####testing
# lx, ly, lz = 100, 100, 2
# data = xr.DataArray(np.random.rand(lx, lz, ly), coords = {'lon': np.arange(0, lx), 'time': [datetime.datetime(2024,1,2), datetime.datetime(2024,1,3)], 'lat': np.arange(0, ly)})
# filename = 'thrash/save_array_test.nc'
# unlimited_dim = 'time'
# overwrite = True

# Path(filename).unlink()

# with netCDF4.Dataset(filename, mode='a') as nc:
#     print(nc)
#     print(nc['time'])
#     print(nc['time'][:])

# with xr.open_dataset(filename) as ds:
#     print(ds['var'].max(['lon', 'lat']))
####

def save_array(data, filename, unlimited_dim = None, overwrite = False):

    if not isinstance(unlimited_dim, str):
        raise ValueError(f'Unlimited_dim must be a string! Got {type(unlimited_dim)}')
    if unlimited_dim not in data.dims:
        raise ValueError(f'Unlimited_dim must be a dimension of data. Got {unlimited_dim}')
    if isinstance(data, xr.DataArray):
        if data.name is None:
            data.name = 'var'
        data = data.to_dataset()
    if isinstance(filename, str):
        filename = Path(filename)

    if not filename.is_file():
        data.to_netcdf(filename, mode='w', unlimited_dims = [unlimited_dim]) #, encoding = {param: dict(zlib = True, complevel = 9)}
    else:
        _append_to_netcdf(filename, data, unlimited_dim = unlimited_dim, overwrite = overwrite)

# based on: https://github.com/pydata/xarray/issues/1672
def _append_to_netcdf(filename, ds_to_append, unlimited_dim, overwrite = False):

    with netCDF4.Dataset(filename, mode='a') as nc:

        nc_coord = nc[unlimited_dim]

        if ds_to_append[unlimited_dim].dtype == np.dtype('<M8[ns]'):
            ##Transform datetime values to netCDF units
            dt_coords = [
                datetime.datetime(j, m, d)
                for d, m, j in zip(
                    ds_to_append[unlimited_dim].dt.day.values,
                    ds_to_append[unlimited_dim].dt.month.values,
                    ds_to_append[unlimited_dim].dt.year.values,
                )
            ]
            dt_num = netCDF4.date2num(
                dt_coords, units=nc_coord.units, calendar=nc_coord.calendar
            )
        else:
            dt_num = list( ds_to_append[unlimited_dim].values )

        contained_dt = [i in nc_coord[:] for i in dt_num]

        if not overwrite:
            if all(contained_dt):
                raise ValueError(f"All slices in dimension {unlimited_dim} already contained in output file!")
            elif any(contained_dt):
                warnings.warn(f"Some slices of dimension {unlimited_dim} already contained in output file! Only new slices are written.")

                ##Remove already contained coords
                ds_to_append = ds_to_append.sel({unlimited_dim: [not i for i in contained_dt]})
                dt_num = [dt_num[i] for i,j in enumerate(contained_dt) if not j]

        for var_name in ds_to_append.keys():

            if not var_name in nc.variables:
                #TODO: Support adding new variables to file
                warnings.warn(f"Variable {var_name} not found in existing nc file. Adding new variables is not supported.")
                continue

            expand_data = ds_to_append[var_name]
            nc_variable = nc[var_name]

            # Ensure the same encoding as was previously stored.
            if hasattr(nc_variable, 'calendar'):
                expand_data.encoding = {
                    'units': nc_variable.units,
                    'calendar': nc_variable.calendar,
                }

            data_encoded = xr.conventions.encode_cf_variable(expand_data.variable)

            nc_idx = []
            l = 0
            for i in dt_num:
                if i in nc_coord[:]:
                    nc_idx.append([c for c,j in enumerate(nc_coord[:]) if j == i][-1])
                else:
                    nc_idx.append(len(nc_coord[:])+l)
                    l += 1

            left_slices = list(nc_variable.dimensions).index(unlimited_dim)
            nc_slice = (
                (slice(None),) * left_slices
                + (nc_idx, )
                + (slice(None),) * (len(nc.dimensions) - left_slices - 1)
            )
            nc_variable[nc_slice] = data_encoded.transpose(*nc_variable.dimensions).data
            nc_coord[nc_idx] = dt_num
