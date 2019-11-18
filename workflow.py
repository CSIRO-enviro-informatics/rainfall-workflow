"""!

Main file. Contains highest level functions to execute major jobs.

Inputs:
	- SMIPS rainfall 1km grid - predictand (DV) - source: CSIRO
	- ACCESS-G or -R ~25x39 km grid - predictor (IV) - source: NCI

Jobs:
	- Re-grid SMIPS data from 1km grid to ACCESS-G size grids
  - Grab the latest matching data ACCESS-G and SMIPS file
	- For each grid point: Create post-processed forecast
		○ Extract symmetric grid point(s) from predictand and predictor and transform
		○ Model fit/forecast
			§ Transform predictor and predictand time series to normal distributions
			§ RPP-SC - call fit() during training , and forecast() during regular use
				□ Model parameters from fit() are saved and loaded to forecast()
			§ Output: post-processed 7 day time series forecast for grid point.
				□ Dimensions: lead time (9), ensemble member (1000)
	- Reassemble grid
	- For each grid point: restore spatial correlations
		○ Shuffle - restore spatial correlations
			§ Output: shuffled 7 day forecast for grid point
	- Hydrological model
		○ Output: 7 day ensemble soil moisture forecast (API)
"""

import datetime

import data_transfer
import iris_regridding
import transform
import source_cube, forecast_cube, parameter_cube
import settings
import numpy as np
from netCDF4 import Dataset


# nsw bounding coords
top_lat = -27.9675 # lat upper bound
bot_lat = -37.6423 # lat lower bound
left_lon = 140.6947 # lon lower bound
right_lon = 153.7687 # lon upper bound


def check_for_bad_smips():
    """!
    Attempt to deal with an error in the SMIPS data by not using data with abnormally high values.
    """
    fname = settings.SMIPS_AGG
    cube = Dataset(fname, mode='a', format='NETCDF4')
    prcp = cube.variables['blended_precipitation']

    for i in range(cube.dimensions['time'].size):
        m = np.nanmax(prcp[i])
        if m > 900:
            print(i, np.nanmax(prcp[i]))
            prcp[i] = -9999.0

    cube.close()


def daily_jobs():
    """!
    Get all new daily data - should be done on a regular automatic basis.
    1. Get latest ACCESS-G files from NCI
    2. Regrid latest SMIPS files
    3. Aggregate new ACCESS-G and SMIPS files into ACCESS-G.nc and SMIPS.nc respectively
    """
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    source_cube.aggregate_netcdf(smips=True)
    source_cube.aggregate_netcdf(accessg=True)
    print('Daily jobs done')


def create_parameter_files():
    """!
    Generate forecast parameters and save to netCDF files.
    """
    lats, lons = source_cube.get_lat_lon_values()

    for lat in lats:
        if bot_lat <= lat <= top_lat :
            for lon in lons:
                if left_lon <= lon <= right_lon:
                    try:
                        parameter_cube.generate_forecast_parameters(lat, lon)
                    except ValueError:  # coordinates don't have data or don't have a recognized timezone
                        continue
    parameter_cube.aggregate_netcdf()


def grid_forecast(date: datetime.date, lat: float, lon: float):
    """!
    Create a forecast for a single set of coordinates and save to netCDF.
    @param date: date to forecast
    @param lat: latitude value
    @param lon: longitude value
    """
    mu, cov, tp = parameter_cube.read_parameters(lat, lon)
    transform.transform_forecast(lat, lon, date, mu, cov, tp)


def create_forecast_files(date: datetime.date):
    """!
    Read the ACCESS-G data for a given forecast date, transform it using the predictor transformation parameters saved
    to netcdf, and save the resulting forecast to netcdf.
    - for all lead times for a date
    - for all grid points
    @param date date to forecast
    """
    lats, lons = source_cube.get_lat_lon_values()

    for lat in lats:
        if bot_lat <= lat <= top_lat:
            for lon in lons:
                if left_lon <= lon <= right_lon:
                    try:
                        grid_forecast(date, lat, lon)
                    except ValueError:
                        continue
    forecast_cube.aggregate_netcdf(date)


def create_shuffled_forecasts():
    """!
    Create shuffled forecasts for all coordinates and save to netCDF.
    """
    date_index_sample = source_cube.sample_date_indices()  # need this to be created once and then always be the same
    lats, lons = source_cube.get_lat_lon_values()

    for lat in range(len(lats)):
        if bot_lat <= lats[lat] <= top_lat :
            for lon in range(len(lons)):
                if left_lon <= lons[lon] <= right_lon:
                    transform.shuffle(lat, lon, date_index_sample)

    forecast_cube.aggregate_netcdf(settings.placeholder_date, settings.FORECAST_SHUFFLE_PATH)


if __name__ == '__main__':
    daily_jobs()
    check_for_bad_smips()
    create_parameter_files()
    create_forecast_files(settings.placeholder_date)
    create_shuffled_forecasts()
    print('Done')
