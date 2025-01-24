import xarray as xr
import rioxarray
from rasterio.enums import Resampling
from pydist.base_logger import logger

def align_arrays(*objects, base, method = 'bilinear', x_dim = 'lon', y_dim = 'lat'):

    res_methods = {
        'nearest': Resampling.nearest,
        'bilinear': Resampling.bilinear,
        'cubic': Resampling.cubic
    }

    out = []
    for arr in objects:

        if (arr.rio.crs != base.rio.crs) or (arr.rio.resolution() != base.rio.resolution()):

            logger.debug('Reprojecting climate data')

            arr = (
                arr.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
                .rio.reproject_match(base, resampling = res_methods[method])
                .rename({"x": x_dim, "y": y_dim})
            )

        arr_re = arr.assign_coords({
            x_dim: base[x_dim],
            y_dim: base[y_dim],
        })

        out.append(arr_re)

    return(tuple(out))
