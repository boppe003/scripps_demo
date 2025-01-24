import xesmf as xe
import xarray as xr
import numpy as np
import os
from dataclasses import dataclass

@dataclass
class Bounds:
    min_lat:  int | np.floating[any]
    max_lat:  int | np.floating[any]
    min_lon:  int | np.floating[any]
    max_lon:  int | np.floating[any]
    lat_step: int | np.floating[any]
    lon_step: int | np.floating[any]


# this only works for certain OBDAC ocean products. NASA is working to make more products uniform though
def regrid(file: os.PathLike[any] , bounds: Bounds, method: str = "bilinear") -> xr.Dataset:
    navigation_data = xr.open_dataset(file, group="navigation_data")
    geophysical_data = xr.open_dataset(file, group="geophysical_data")
    attrs_data =  xr.open_dataset(file)
    src_data = xr.merge([geophysical_data, navigation_data, attrs_data], combine_attrs="drop_conflicts")
    src_data = src_data.set_coords(navigation_data.data_vars)
    del navigation_data, geophysical_data, attrs_data

    target_lats = np.arange(bounds.min_lat, bounds.max_lat, bounds.lat_step, dtype = np.float32)
    target_lons = np.arange(bounds.min_lon, bounds.max_lon, bounds.lon_step, dtype = np.float32)

    src_grid = xr.Dataset({"latitude":(src_data.dims, src_data.latitude.values), "longitude":(src_data.dims, src_data.longitude.values)})
    target_grid = xr.Dataset({"latitude": ("latitude", target_lats), "longitude": ("longitude", target_lons)})

    regridder = xe.Regridder(src_grid, target_grid, method=method)
    array = regridder(src_data, keep_attrs=True)
    

    #formats 0's back to nan
    return array.where(array != 0, other=np.nan)

#in  past project I downloaded a point subset. and left this snippet
def point_regrid(file: os.PathLike[any]) -> xr.Dataset:
    navigation_data = xr.open_dataset(file, group="navigation_data")
    geophysical_data = xr.open_dataset(file, group="geophysical_data")
    
    src_data = xr.merge([navigation_data, geophysical_data])
    return src_data
