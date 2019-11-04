from source_cube import get_lat_lon_values
import os
from netCDF4 import Dataset
import datetime
import settings
import xarray as xr
import glob
import numpy as np

aggregated_params = 'PARAMS_aggregated.nc'


# create cube for grid parameters
# also check paramaters if you're creating a single-grid cube or a whole-grid cube for aggregation
# Lat and lon are optional bc not needed for aggregated file

def create_cube(cubepathname, lat=None, lon=None):
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')

    outcube.history = 'Created ' + datetime.datetime.now().isoformat()

    outcube.createDimension('np_set', 2000) # may be 2000
    outcube.createDimension('tp_set', 2)
    np_set = outcube.createVariable('np_set', 'u4', 'np_set')
    tp_set = outcube.createVariable('tp_set', 'u4', 'tp_set')
    np_set.setncatts({"long_name": "normalised parameter set"})
    tp_set.setncatts({"long_name": "transformed parameter set"})

    outcube.createDimension('np_types', 5)
    outcube.createDimension('tp_types', 3)
    np_types = outcube.createVariable('np_types', 'u4', 'np_types')
    tp_types = outcube.createVariable('tp_types', 'u4', 'tp_types')
    np_types.setncatts({"long_name": "normalised parameter types: mu1, mu2, sigma1, sigma2, scaling_factor"})
    tp_types.setncatts({"long_name": "transformed parameter types: lambda, epsilon, scaling_factor"})

    outcube.createDimension('lead_time', 9)
    lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

    lead[:] = range(9)
    np_set[:] = range(2000)
    tp_set[:] = range(2)
    np_types[:] = range(5)
    tp_types[:] = range(3)

    if 'aggregate' in cubepathname:
        #refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
        lat, lon = get_lat_lon_values()
        rows = len(lat)
        cols = len(lon)

        outcube.description = 'Normal and transformed parameters for entire grid.'

        outcube.createDimension('lon', cols)  # cols
        outcube.createDimension('lat', rows)  # rows
        ylat = outcube.createVariable('lat', 'f4', 'lat')
        xlon = outcube.createVariable('lon', 'f4', 'lon')

        # data variables
        outcube.createVariable('n_parameters', 'f8', ('lat', 'lon', 'lead_time', 'np_set', 'np_types'),
                               least_significant_digit=3, fill_value=-9999.0)
        outcube.createVariable('t_parameters', 'f8', ('lat', 'lon', 'lead_time', 'tp_set', 'tp_types'),
                               least_significant_digit=3, fill_value=-9999.0)

    else:
        if not lat or not lon:
            print('Need to run with lat/lon parameters')

        outcube.description = 'Normal and transformed parameters for grid at: ' + lat + ', ' + lon
        # data variables
        outcube.createVariable('n_parameters', 'f8', ('lead_time', 'np_set', 'np_types'), least_significant_digit=3,
                               fill_value=-9999.0)
        outcube.createVariable('t_parameters', 'f8', ('lead_time', 'tp_set', 'tp_types'), least_significant_digit=3,
                               fill_value=-9999.0)
        ylat = outcube.createVariable('lat', 'f4')
        xlon = outcube.createVariable('lon', 'f4')

    ylat[:] = lat
    xlon[:] = lon

    # add attributes
    xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
    ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

    outcube.close()


def add_to_netcdf_cube(cubename, lead_time, normal_data, transformed_data):
    """
        if params: Adds params data to a single grid cube.
        if forecast: Adds forecast data to a single grid cube
        :param cubename: name of the cube - will contain 'params' or 'forecast', and lat and lon info
        :param normal_data: param data to add; [1000, 5] OR forecast data to add; [1000]
        :param lead_time: lead time in days; in range(9)
        :param transformed_data: param data to add; [4]; params only
        :param date: date for forecast only
        :return: void
        """

    cubepathname = os.path.join(settings.PARAMS_GRIDS_PATH, cubename)
    _, lat, lon = cubename.rstrip('.nc').split('_')
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname, lat=lat, lon=lon)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    norm = outcube.variables['n_parameters']
    norm[lead_time, :] = normal_data[:]
    trans = outcube.variables['t_parameters']
    trans[lead_time, :] = transformed_data[:]

    outcube.close()


def add_to_netcdf_cube_from_files(files, cubename):
    # add parameter data to aggregate netcdf cube
    cubepathname = os.path.join(settings.PARAMS_PATH, cubename)
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
        normal_data = dataset['n_parameters'].values
        transformed_data = dataset['t_parameters'].values
        norm_datain = np.where(normal_data == 9.96921e+36, -9999.0, normal_data)
        trans_datain = np.where(transformed_data == 9.96921e+36, -9999.0, transformed_data)

        print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)
        lat_index = lat_indices[lat]
        lon_index = lon_indices[lon]
        norm = outcube.variables['n_parameters']
        norm[lat_index, lon_index, :] = norm_datain[:]
        trans = outcube.variables['t_parameters']
        trans[lat_index, lon_index, :] = trans_datain[:]
    outcube.close()


def aggregate_netcdf():
    # aggregate parameter files
    path = settings.PARAMS_GRIDS_PATH
    files = [file for file in glob.glob(path + '*.nc')]
    add_to_netcdf_cube_from_files(cubename=aggregated_params, files=files)