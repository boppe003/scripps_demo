# %%
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import xarray as xr
import numpy as np
import scipy.stats as stats

# %%
wind_ds = xr.open_mfdataset('data/CYGNSS_NOAA_L2_SWSP_25KM_V1.2_1.2-20250124_000751/regridded/*.nc', concat_dim='time', combine='nested')
wind_ds = wind_ds.sel(time=slice("2024-03-05", "2024-12-31"))

# %%
ocean_ds = xr.open_mfdataset("data/PACE_OCI_L2_BGC_NRT_2.0-20250121_161459/regridded/*.nc")
ocean_ds = ocean_ds.sel(time=slice('2024-01-01', '2024-12-31'))

lat_factor, lon_factor = ocean_ds.latitude.size/wind_ds.latitude.size, ocean_ds.longitude.size/wind_ds.longitude.size 
lat_factor, lon_factor = round(lat_factor), round(lon_factor)

# temporal: by day
ocean_ds = ocean_ds.resample(time='D').mean()

binned_wind_ds = wind_ds.resample(time='D').mean()
# # spatial: same size as wind ds.
binned_ocean_ds = ocean_ds.coarsen(latitude=lat_factor, longitude=lon_factor, boundary="trim").mean()
binned_ocean_ds = binned_ocean_ds.reindex_like(binned_wind_ds, method='nearest')
del ocean_ds, wind_ds

stats_ds = xr.Dataset(
    data_vars = {
        "carbon_phyto": binned_ocean_ds['carbon_phyto'],
        "wind_speed": binned_wind_ds['wind_speed']
    }
)
template_array = np.full((stats_ds.latitude.size, stats_ds.longitude.size), (np.nan), np.float32)

stats_ds['linear_stats_array'] = (["latitude", "longitude"] ,template_array.copy())
stats_ds['n_points'] = (["latitude", "longitude"], template_array.copy())

stats_ds['slope'] = (["latitude", "longitude"], template_array.copy())
stats_ds['intercept'] = (["latitude", "longitude"], template_array.copy())
stats_ds['r_value'] = (["latitude", "longitude"], template_array.copy())
stats_ds['p_value'] = (["latitude", "longitude"], template_array.copy())
stats_ds['std_err'] = (["latitude", "longitude"], template_array.copy())


stats_ds = stats_ds.chunk({'latitude': 10, 'longitude': 10, 'time': -1})

del template_array
# %%

groups = stats_ds.groupby(['latitude', 'longitude'])
groups

# %%
for name, group in groups:
    # print(f"starting group:{name}...")
    mask = ~np.isnan(group.wind_speed) & ~np.isnan(group.carbon_phyto)
    wind_speed, carbon_phyto = group.wind_speed.values[mask], group.carbon_phyto.values[mask]
    lat, lon = name  
    n_points = len(wind_speed)
    stats_ds['n_points'].loc[dict(latitude=lat, longitude=lon)] = n_points
    if len(wind_speed) < 2 or len(carbon_phyto) < 2:
        print(f"{name} had {n_points} values, no calculation")
        continue  # Skip if not enough data for regression

    try:

        slope, intercept, r_value, p_value, std_err = stats.linregress(wind_speed, carbon_phyto)

        stats_ds['slope'].loc[dict(latitude=lat, longitude=lon)] = slope
        stats_ds['intercept'].loc[dict(latitude=lat, longitude=lon)] = intercept
        stats_ds['r_value'].loc[dict(latitude=lat, longitude=lon)] = r_value
        stats_ds['p_value'].loc[dict(latitude=lat, longitude=lon)] = p_value
        stats_ds['std_err'].loc[dict(latitude=lat, longitude=lon)] = std_err
        print(f"{name} had {n_points} values, regression was calculated")


    except ValueError:
        
        stats_ds['slope'].loc[dict(latitude=lat, longitude=lon)] = np.nan
        stats_ds['intercept'].loc[dict(latitude=lat, longitude=lon)] = np.nan
        stats_ds['r_value'].loc[dict(latitude=lat, longitude=lon)] = np.nan
        stats_ds['p_value'].loc[dict(latitude=lat, longitude=lon)] = np.nan
        stats_ds['std_err'].loc[dict(latitude=lat, longitude=lon)] = np.nan
        print(f"{name} had {n_points} values, regression resulted in an error")

stats_ds.to_netcdf("data/stats_wind_speed_phyto_carbon_ds.nc")
