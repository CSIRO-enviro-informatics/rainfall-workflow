	# 1. Grab the layer (2D array representing one time step) from the ACCESS-G container
	# 	○ One timestep at a time because this'll need to happen in the future when daily data is coming in.
    #   - and because we need to get a smaller container for iris
	# 2. Resample it (in Python if you can)
	# 	○ Using Iris, (example code in regrid_silo_to_era5_grid.py)
	# 3. Push it into a new NetCDF container (code in matt_resampling)
	# 4. Drill the container to get your timeseries for each pixel (grid centroid)
	# 5. Store the time series in fluxDB

import iris
import settings
import xarray as xr
import datetime
import numpy as np

smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER  # original
 # target

# Extract and treat each day seperately - for the future, and because the SMIPS file is so big anything else would cause memory errors
def extract_timestep(nc, date):
    ncp = nc['Blended Precipitation']
    ncpd = ncp.sel(time=date)
    ncpd.name = 'blended_precipitation'
    cube = ncpd.to_iris()
    return cube


# Save the resampled netcdf file containing a day and only blended_precipitation
def save_timestep(cube, date):
    new_file = 'test/SMIPS_'+str(date)+'.nc'
    iris.save(cube, new_file)


# Resample SMIPS grids to same shape as ACCESS-G
def regrid(cube, target_file):
    target = iris.load(target_file)
    resampled = cube.regrid(target)
    return resampled


# Because the date from xarray is an np.datetime64 object
def convert_date(date):
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
    dt_date = datetime.datetime.utcfromtimestamp(timestamp)
    return dt_date.date()


if __name__ == '__main__':
    nc = xr.open_dataset(smips_file)
    print(nc.time)
    for date in nc.time:
        date = convert_date(date)
        timestep = extract_timestep(nc, date)
        access_g_file = settings.ACCESS_G_PATH + settings.access_g_filename(date)  # target
        regridded = regrid(timestep, access_g_file)
        save_timestep(regridded, date)


    #date = datetime.date(2015, 11, 20)

#bp_constraint = iris.Constraint(name='Blended_Precipitation')
#print(smips_file)
#smips_cubes = iris.load(smips_file)
#print(smips_cubes)