from pathlib import Path
import netCDF4
from datetime import datetime as dt
def find_groups(filename: Path| str) -> list:
    """Returns the groups in a netCDF file"""

    # ensure is path
    if(type(filename) == str):
        filename = Path(filename)
        
    with netCDF4.Dataset(filename, "r") as data:
        return data.groups.keys()


def is_valid_iso_timestamp(iso_time: str) -> bool:
    """Checks if a string is in the format YYYYMMDDTHHMMSS."""
    try:
        dt.strptime(iso_time, "%Y%m%dT%H%M%S")
        return True
    except ValueError:
        return False


def file_to_time_id(filename: Path | str) -> str:
    """returns the time in iso format see https://oceancolor.gsfc.nasa.gov/resources/docs/filenaming-convention/ for details.
    its also worth noting that file metadata (attrs) include precise starttime and endtime values"""
    #ensure is path
    if(type(filename) == str):
        filename = Path(filename)

    # if its a path ensure we only get the actual filename
    # /data/a/a/a/a/filename.txt -> filename.txt
    filename = filename.name

    if(filename.count('.') > 1):
        #if there is more then one dot, assume its nasa filename
        time_id = filename.split(".")[1]
    
    else: #assumes its just timestamp.nc
        time_id = filename.split(".")[0]

    return(time_id)


def list_time_stamps_from_folder(folder: Path | str) -> list:
    if type(folder) == str:
        folder = Path(folder)
    """takes all the files in `folder` and returns list of all the time stamps"""
    return [file_to_time_id(file) for file in folder.glob("*.nc")] 


def timestamp_to_datetime(file:str):
    timestamp = file_to_time_id(file)
    return dt.strptime(timestamp, "%Y%m%dT%H%M%S")

