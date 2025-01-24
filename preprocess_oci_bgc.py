import utils
import os
import remap_util as rmp
import xarray as xr
from pathlib import Path
import numpy as np
import multiprocessing

#timestamps are stored in the folder name so we only match the first part.
inpath = list(Path('data/').glob("PACE_OCI_L2_BGC_NRT_2.0*"))[0]
outpath = Path(inpath / 'regridded')
outpath.mkdir(exist_ok=True)




# Northeast corner: 8.5° N, 135.5° E
# Southwest corner: 4.0° N, 130.5° E

bounds = rmp.Bounds(
    min_lon = 130.5,
    max_lon = 135.5,
    min_lat = 4.0,
    max_lat = 8.5,
    lat_step=None,
    lon_step=None,
)


#finding the average step size for latitude and longitude.
def get_avg_distance(array: np.ndarray) -> float:
    # Difference along rows (north-south)
    south = np.abs(np.diff(array, axis=0))  
    # Difference along columns (east-west)
    east = np.abs(np.diff(array, axis=1))   

    distance_sum = np.sum(south) + np.sum(east)

    # Cumulative number of distances
    distances = south.size + east.size

    return distance_sum / distances

def process_swath(file: str) -> tuple:
    ds = xr.open_dataset(file, group = 'navigation_data')
    # Compute average distance for latitude and longitude
    latitude_avg = get_avg_distance(ds['latitude'].values)
    longitude_avg = get_avg_distance(ds['longitude'].values)
    return latitude_avg, longitude_avg  

def main():
    file_list = list(inpath.glob('*.nc'))

    with multiprocessing.Pool() as pool:
        results = pool.map(process_swath, file_list)

    sum_latitude = sum(latitude for latitude, _ in results)
    sum_longitude = sum(longitude for _, longitude in results)

    latitude_avg = sum_latitude / len(file_list)
    longitude_avg = sum_longitude / len(file_list)

    return latitude_avg, longitude_avg

bounds.lat_step , bounds.lon_step = main()

def regrid_file(file: os.PathLike[any]):
    array = rmp.regrid(file, bounds=bounds)
    name = utils.file_to_time_id(file)
    time = utils.timestamp_to_datetime(file)
    array = array.expand_dims(time=[time])
    array.to_netcdf(outpath / f"{name}.nc")
    del array, time
    print(name)


def remove_files_already_done():
    file_id_pairs = { utils.file_to_time_id(file_name):file_name for file_name in inpath.glob('*.nc*')}
    completed_ids =  [ utils.file_to_time_id(file_name) for file_name in outpath.glob('*.nc')]

    for id in completed_ids:
        if id in file_id_pairs:
            del file_id_pairs[id]

    return list(file_id_pairs.values())


files_to_process = remove_files_already_done()


with multiprocessing.Pool(2) as pool:
    pool.map(regrid_file, files_to_process)
