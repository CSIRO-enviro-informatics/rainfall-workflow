	# 1. Grab the layer (2D array representing one time step) from the ACCESS-G container
	# 	○ One timestep at a time because this'll need to happen in the future when daily data is coming in.
    #   - and because we need to get a smaller container for iris
	# 2. Resample it (in Python if you can)
	# 	○ Using Iris, (example code in regrid_silo_to_era5_grid.py)
	# 3. Push it into a new NetCDF container (code in matt_resampling)
	# 4. Drill the container to get your access-g timeseries for each pixel (grid centroid)
	# 5. Store the time series in fluxDB

import iris
import settings
import xarray as xr
import datetime
import numpy as np
from data_transfer import create_str_date

smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER  # original
smips_nc = xr.open_dataset(smips_file)
min_lat = min(smips_nc.lat.values)
max_lat = max(smips_nc.lat.values)
min_lon = min(smips_nc.lon.values)
max_lon = max(smips_nc.lon.values)


# Extract and treat each day seperately - for the future, and because the SMIPS file is so big anything else would cause memory errors
def extract_timestep(nc, date):
    ncp = nc['Blended Precipitation']
    ncpd = ncp.sel(time=date)
    ncpd.name = 'blended_precipitation'
    cube = ncpd.to_iris()
    return cube


# Save the resampled netcdf file containing a day and only blended_precipitation
def save_timestep(cube, str_date):
    new_file = 'test/SMIPS_blnd_prcp_regrid_'+ str_date +'.nc'
    iris.save(cube, new_file)
    print(new_file + ' saved')


# Creating the regridder is most resource intensive part, so create only once
def init_regridder():
    # Random files to initialise the regridder
    target_file = settings.ACCESS_G_PATH + settings.access_g_filename('20190101') # random access-g file
    cube = extract_timestep(smips_nc, datetime.date(2019, 1, 1))  # random smips file

    target = iris.load_cube(target_file, 'accumulated precipitation') #, constraint)

    target.coord('longitude').guess_bounds()
    target.coord('latitude').guess_bounds()

    cube.coord('longitude').guess_bounds()
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord('latitude').standard_name = 'latitude'

    regridder = iris.analysis.AreaWeighted().regridder(cube, target)
    return regridder


# Resample SMIPS grids to same shape as ACCESS-G
def regrid(cube, regridder):
    cube.coord('longitude').guess_bounds()
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord('latitude').standard_name = 'latitude'
    return regridder(cube)


# Because the date from xarray is an np.datetime64 object
def convert_date(date):
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
    dt_date = datetime.datetime.utcfromtimestamp(timestamp)
    return dt_date.date()


if __name__ == '__main__':
    regridder = init_regridder()
    for date in smips_nc.time:
        date = convert_date(date)  # from np datetime to datetime datetime
        str_date = create_str_date(date.year, date.month, date.day)  # for file name
        timestep = extract_timestep(smips_nc, date)  # this is the daily smips cube
        regridded = regrid(timestep, regridder)  # regridding to match access-g shape
        save_timestep(regridded, str_date)  # save to disk
