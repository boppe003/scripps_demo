
from remap_util import Bounds
import xarray as xr
import numpy as np
import pandas as pd
from multiprocessing import Pool
from pathlib import Path

bounds = Bounds(
    min_lon = 130.5,
    max_lon = 135.5,
    min_lat = 4.0,
    max_lat = 8.5,
    lat_step= np.float32(0.224611),
    lon_step= np.float32(0.224580),
)
#timestamps are stored in the folder name so we only match the first part.
inpath = list(Path('data/').glob("CYGNSS_NOAA_L2_SWSP_25KM*"))[0]
outpath = Path(inpath / 'regridded')
outpath.mkdir(exist_ok=True)

def create_matrix_shape(bounds: Bounds):
    """create a range of x and y values using the given boarders and the given cell size
    
    returns two pd.Series of latitude, longitude"""
    latitude = pd.Series(np.arange(bounds.min_lat, bounds.max_lat, bounds.lat_step, dtype=np.float32))
    longitude = pd.Series(np.arange(bounds.min_lon, bounds.max_lon, bounds.lon_step, dtype=np.float32))
    return latitude, longitude

def prep_cygnss_dataframe(ds):
    """opens the xr dataset, formats into a pandas dataframe and formats time column correctly.
   
    returns the formated dataframe and the variable attributes in a dict"""
    
    if not type(ds) == xr.Dataset:
        ds= xr.open_dataset(ds)

    variable_attributes = {}
    for var in ds:
        variable_attributes[var] = ds[var].attrs

    df = ds.to_dataframe()
    df.attrs = ds.attrs

    #format time elements correctly
    df['sample_time'] =  pd.to_datetime(df['sample_time'])
    
    return df, variable_attributes


def process_remap_indexes(latitude, longitude, df):
    """"""
    #find all the distances
    lat_diff = (latitude.values.reshape(-1, 1) - df['lat'].values)  
    lon_diff = (longitude.values.reshape(-1, 1) - df['lon'].values) 

    #get the index of the lowest distance for each variable.
    lat_idx = abs(lat_diff).argmin(axis=0)
    lon_idx = abs(lon_diff).argmin(axis=0)

    #remap the correct latitude, longitude to the original dataframe
    df['lat'] = latitude.iloc[lat_idx].values
    df['lon'] = longitude.iloc[lon_idx].values
    return df


def create_variable_arrays(latitude, longitude, variables : list = ['wind_speed', 'wind_speed_uncertainty']):
    """creates np.nan arrays of the correct size for each variable name

    returns each array as a dict of {var_name: array}"""

    shape = (latitude.size, longitude.size)
    return {var: np.full(shape, np.nan, dtype=np.float32) for var in variables}


def map_to_variables(latitude, longitude, df, variable_arrays):

    for _, row in df.iterrows():

        lat_idx = int(abs(latitude - row['lat']).argmin())
        lon_idx = int(abs(longitude - row['lon']).argmin())  

        for var in variable_arrays.keys():
            #no value assign value
            if np.isnan(variable_arrays[var][lat_idx, lon_idx]):
                variable_arrays[var][lat_idx, lon_idx] = row[var]
            #if there is already a value we take the average
            else:
                variable_arrays[var][lat_idx, lon_idx] = np.mean([variable_arrays[var][lat_idx, lon_idx], row[var]])

    return variable_arrays

def file_to_time_id(filename: Path | str) -> str:
    #ensure is path
    if not type(filename) == Path:
        filename = Path(filename)


    #final path component, if any
    filename = filename.name
    if filename.count('.') <= 1:
        time_id = filename.split('.')[0]

    else:
        # -> 19990101-123456
        time_id = filename.split(".")[2][18:]
        # -> 19990101T123456
        time_id = time_id.replace('-','T')
    return time_id


def main(file):
    df, variable_attributes = prep_cygnss_dataframe(file)
    df = process_remap_indexes(latitude, longitude, df)
    
    variable_arrays = map_to_variables(latitude, longitude, df, variable_arrays_template.copy())

    out_ds = xr.Dataset(
    coords= {
        'latitude':('latitude', latitude),
        'longitude':('longitude', longitude),
        },
        attrs=df.attrs
    )
    out_ds = out_ds.expand_dims(time=[df.sample_time.values[0]])

    for var in variable_arrays.keys():
        out_ds[var] = (['latitude', 'longitude'], variable_arrays[var])
        out_ds[var].attrs  = variable_attributes[var]

    name = file_to_time_id(file)
    

    out_ds.to_netcdf(outpath/ f"{name}.nc")
    print(name)




def remove_files_already_done():
    file_id_pairs = { file_to_time_id(file_name):file_name for file_name in inpath.glob('*.nc4')}
    completed_ids =  [ file_to_time_id(file_name) for file_name in outpath.glob('*.nc')]

    for id in completed_ids:
        if id in file_id_pairs:
            del file_id_pairs[id]

    return list(file_id_pairs.values())


files_to_process = remove_files_already_done()
latitude, longitude = create_matrix_shape(bounds)
variable_arrays_template = create_variable_arrays (latitude, longitude) # variables = ['wind_speed', 'wind_speed_uncertainty']


with Pool(processes=4) as pool:
    pool.map(main, files_to_process)


