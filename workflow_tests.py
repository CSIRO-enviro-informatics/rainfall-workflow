"""!
Tests to ease the creation and manual validation of netCDF4 files).
"""

import os
import settings
import datetime
import parameter_cube, forecast_cube, source_cube
import transform
import xarray as xr
import netCDF4
from dates import date2str
import workflow

#test_coords = [-36.5625, 145.54688]
#test_coords = [-34.6875, 143.08594]
#test_coords = [-36.5625, 147.3047] # infinity - fixed
test_coords = [-35.85938, 148.3594] # anomaly over canberra
test_date = workflow.placeholder_date

# CAUTION: test functions will delete the files they are overwriting


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)
    else:
        print('File ' + file + ' does not exist to be deleted')


def test_parameter_file_creation():
    lat = test_coords[0]
    lon = test_coords[1]
    fname = settings.params_filename(lat, lon)
    delete_file(settings.PARAMS_GRIDS_PATH + fname)
    parameter_cube.generate_forecast_parameters(lat, lon)


def test_parameter_aggregation():
    fname = settings.PARAMS_AGG
    delete_file(fname)
    parameter_cube.aggregate_netcdf()


def test_forecast_file_creation():
    lat = test_coords[0]
    lon = test_coords[1]
    mu, cov, tp = parameter_cube.read_parameters(lat, lon)
    fname = settings.forecast_filename(test_date, lat, lon)
    delete_file(settings.FORECAST_GRID_PATH + fname)
    transform.transform_forecast(lat, lon, test_date, mu, cov, tp)


def test_forecast_aggregation():
    datestr = date2str(test_date)
    fname = settings.forecast_agg(datestr)
    delete_file(fname)
    forecast_cube.aggregate_netcdf(test_date)


def test_shuffle():
    date_sample = source_cube.sample_date_indices()
    lat_dict, lon_dict = source_cube.get_lat_lon_indices()
    lat = lat_dict[round(float(test_coords[0]), 2)]
    lon = lon_dict[round(float(test_coords[1]), 2)]
    delete_file(settings.shuffled_forecast_filename(test_date, lat, lon))
    transform.shuffle(lat, lon, date_sample)


def test_shuffle_aggregation():
    delete_file(settings.shuffled_forecast_agg(test_date))
    forecast_cube.aggregate_netcdf(test_date, settings.FORECAST_SHUFFLE_PATH)


#def test_netcdf_merge():
#    path = 'temp/forecast/grids/*.nc'
#    merged = xr.open_mfdataset(path, combine='nested', concat_dim='coords')
#    merged.to_netcdf(format='NETCDF4', engine=('netcdf4'))  # not seeing my netcdf library???
