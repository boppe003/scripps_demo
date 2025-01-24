import matplotlib.pyplot as plt
import matplotlib.animation as animation
import xarray as xr

ocean_ds = xr.open_mfdataset("data/PACE_OCI_L2_BGC_NRT_2.0-20250121_161459/regridded/*.nc")

variable_vbar_bounds = {
    'chlor_a':(0, 5),
    'carbon_phyto':(0, 500),
    'poc':(0, 350)
}
fig, ax = plt.subplots(figsize=(18, 5), ncols=3)

#generates colorbars
for n, variable in enumerate(variable_vbar_bounds):
    vmin, vmax = variable_vbar_bounds[variable]

    ocean_ds[variable].isel(time=0).plot(vmin= vmin, vmax= vmax, ax=ax[n])

def tick_frame(frame):
    for n, variable in enumerate(variable_vbar_bounds):
        vmin, vmax = variable_vbar_bounds[variable]

        ax[n].clear()
        ocean_ds[variable].sel(time= ocean_ds.time[frame]).plot(vmin= vmin, vmax= vmax, ax=ax[n], add_colorbar=False)


ani = animation.FuncAnimation(fig, tick_frame, frames=len(ocean_ds.time), interval=100)
plt.show()
# this bit requires you have ffmpeg installed. it's available through most package managers.
# direct download see, https://ffmpeg.org/ 
ani.save(filename="animation_demo.mp4", writer="ffmpeg") 
