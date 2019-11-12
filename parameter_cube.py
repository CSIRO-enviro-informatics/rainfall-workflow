from source_cube import get_lat_lon_indices, get_lat_lon_values
import os
from netCDF4 import Dataset
import datetime
import settings
import xarray as xr
import glob
import numpy as np
import math
import bjpmodel
import transform


def read_parameters(lat, lon):
    """
    Read the mu, cov, and transform parameter data from netcdf and reconstruct arrays with the same shape and order as returned from BJP
    sample.
    :param: lat, lon - coordinates for the grid whose params to read
    :return:  mu[9][1000][2], cov[9][1000][3], tp[9][2][3]
    """
    file = settings.PARAMS_AGG
    p = xr.open_dataset(file)
    p_grid = p.sel(lat=lat, lon=lon, method='nearest')
    if np.isnan(p_grid['n_parameters']).all():
        raise ValueError('Parameters arrays are empty - write them to file first')

    tp = p_grid['t_parameters']
    nop = p_grid['n_parameters']

    tp[:, 0, 1] = np.array([math.exp(x) for x in tp[:, 0, 1].values])
    tp[:, 1, 1] = np.array([math.exp(x) for x in tp[:, 1, 1].values])

    mu = nop[:, :, :2]
    cov = nop[:, :, 2:]

    return mu, cov, tp


def generate_forecast_parameters(lat, lon):
    """
    For each grid point: Create post-processed forecast
    1. Extract symmetric grid point(s) from observed and forecast and transform
    2. Model fit/forecast
        - Transform predictor and predictand time series to normal distributions
        - TODO: RPP-SC - call fit() during training , and forecast() during regular use
            - Model parameters from fit() are saved and loaded to forecast()
        -  Output: post-processed 9 day time series forecast for grid point.
            - Dimensions: lead time (9), ensemble member (1000)
    """

    start_date = settings.ACCESS_STARTDATE
    end_date = datetime.date(2019, 8, 1)
    fit_data = transform.extract_fit_data(lat, lon, start_date, end_date)

   # Don't process the data if a timezone wasn't found because it's in the ocean
    if fit_data == 'Location over water':
        return

    bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=3000, seed='random')

    for lt in range(9):

        fdata = np.array([fit_data['ptor'][:, lt], fit_data['ptand'][:, lt] ], order='C')

        mu, cov, tparams = bjp_model.sample(fdata)

        # Save mu, cov, and transformation parameters to netcdf file for that grid point
        normal_params = np.concatenate((mu, np.asarray(cov)), axis=1)
        tparams[0, 1] = math.log(tparams[0, 1])
        tparams[1, 1] = math.log(tparams[1, 1])

        add_to_netcdf_cube(settings.params_filename(lat, lon), lt, normal_params, tparams)


def create_cube(cubepathname, lat=None, lon=None):
    # create cube for grid parameters
    # also check paramaters if you're creating a single-grid cube or a whole-grid cube for aggregation
    # Lat and lon are optional bc not needed for aggregated file
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
    #cubepathname = os.path.join(settings.PARAMS_PATH, cubename)
    cubepathname = cubename
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    lat_indices, lon_indices = get_lat_lon_indices()

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
    add_to_netcdf_cube_from_files(files, settings.PARAMS_AGG)
