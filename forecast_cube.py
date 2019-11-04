from source_cube import get_lat_lon_values
import os
from netCDF4 import Dataset
import datetime
import settings
import glob
import xarray as xr
import numpy as np

aggregated_fc = 'FORECAST_aggregated.nc'


def create_cube(cubepathname, date=None, lat=None, lon=None):
    # Lat and lon are optional bc not needed for aggregated file
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')

    outcube.history = 'Created ' + datetime.datetime.now().isoformat()

    outcube.createDimension('ensemble_member', 1000)
    ens = outcube.createVariable('ensemble_member', 'u4', 'ensemble_member')

    outcube.createDimension('lead_time', 9)
    lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

    if 'aggregate' in cubepathname:
        lats, lons = get_lat_lon_values()
        rows = len(lats)
        cols = len(lons)

        outcube.description = 'Normal and transformed parameters for entire grid.'

        outcube.createDimension('lon', cols)  # cols
        outcube.createDimension('lat', rows)  # rows
        ylat = outcube.createVariable('lat', 'f4', 'lat')
        xlon = outcube.createVariable('lon', 'f4', 'lon')

        outcube.createVariable('forecast_value', 'f8', ('lat', 'lon', 'lead_time', 'ensemble_member'), least_significant_digit=3,
                               fill_value=-9999.0)

        outcube.description = "Post-processed forecast for " + str(date)
        ylat[:], xlon[:] = get_lat_lon_values()

    else:
        ylat = outcube.createVariable('lat', 'f4')
        xlon = outcube.createVariable('lon', 'f4')

        outcube.createVariable('forecast_value', 'f8', ('lead_time', 'ensemble_member'), least_significant_digit=3,
                               fill_value=-9999.0)

        outcube.description = 'Post-processed forecast for grid at: ' + lat + ', ' + lon + " on " + str(date)
        ylat[:] = lat
        xlon[:] = lon

    # add attributes
    xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
    ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

    lead[:] = range(9)
    ens[:] = range(1000)

    outcube.close()


def add_to_netcdf_cube(cubename, lead_time, data):

    cubepathname = os.path.join(settings.FORECAST_GRID_PATH, cubename)
    _, lat, lon = cubename.rstrip('.nc').split('_')
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname, lat=lat, lon=lon)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    forecast = outcube.variables['forecast_value']
    forecast[lead_time, :] = data
    outcube.close()


def add_to_netcdf_cube_from_files(files, cubename):
    # aggregate forecasts
    cubepathname = os.path.join(settings.FORECAST_PATH, cubename)
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    lats, lons = get_lat_lon_values()
    lat_indices = {round(float(lat), 2): index for (lat, index) in zip(lats, range(len(lats)))}
    lon_indices = {round(float(lon), 2): index for (lon, index) in zip(lons, range(len(lons)))}

    for file2process in files:
        file = file2process
        _, lat, lon = file.rstrip('.nc').split('_')
        lat, lon = round(float(lat), 2), round(float(lon), 2)  # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup

        dataset = xr.open_dataset(file, decode_times=False)
        forecast_data = dataset['forecast_value'].values
        fc_datain = np.where(forecast_data == 9.96921e+36, -9999.0, forecast_data)

        print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)
        lat_index = lat_indices[lat]
        lon_index = lon_indices[lon]
        data = outcube.variables['forecast_value']
        data[lat_index, lon_index, :] = fc_datain[:]
    outcube.close()


def aggregate_netcdf(date):
    path = settings.FORECAST_GRID_PATH
    files = [file for file in glob.glob(path + '*.nc')]
    create_cube(settings.FORECAST_PATH + aggregated_fc, date)
    add_to_netcdf_cube_from_files(cubename=aggregated_fc, files=files)