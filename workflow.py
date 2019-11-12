# Inputs:
# 	- SMIPS rainfall 1km grid - predictand (DV) - source: CSIRO
# 	- ACCESS-G or -R ~25x39 km grid - predictor (IV) - source: NCI
#
# Jobs:
# 	- Re-grid SMIPS data from 1km grid to ACCESS-G size grid
#   - Grab the latest matching data ACCESS-G and SMIPS file
# 	- For each grid point: Create post-processed forecast
# 		○ Extract symmetric grid point(s) from predictand and predictor and transform
# 		○ Model fit/forecast
# 			§ Transform predictor and predictand time series to normal distributions
# 			§ RPP-SC - call fit() during training , and forecast() during regular use
# 				□ Model parameters from fit() are saved and loaded to forecast()
# 			§ Output: post-processed 7 day time series forecast for grid point.
# 				□ Dimensions: lead time (9), ensemble member (1000)
# 	- Reassemble grid
# 	- For each grid point: restore spatial correlations
# 		○ Shuffle - restore spatial correlations
# 			§ Output: shuffled 7 day forecast for grid point
# 	- Hydrological model
# 		○ Output: 7 day ensemble soil moisture forecast (API)

import datetime

import data_transfer
import iris_regridding
import transform
import source_cube, forecast_cube, parameter_cube
import random
import xarray as xr
import settings
import numpy as np
from shuffle import shuffle_random_ties

placeholder_date = datetime.date(2019, 11, 1)

# nsw bounding coords
# West Bounding Longitude: 140.6947
# East Bounding Longitude: 153.7687
# North Bounding Latitude: -27.9675
# South Bounding Latitude: -37.6423


def shuffle(lat, lon, date_index_sample):
    observed = xr.open_dataset(settings.SMIPS_AGG, decode_times=False)
    fc = xr.open_dataset(settings.forecast_agg(placeholder_date))
    lats, lons = source_cube.get_lat_lon_values()

    obs_pre_shuffle = np.zeros((9, 1000))
    coord_observed = observed.blended_precipitation.values[:, lat, lon]
    fc_pre_shuffle = fc.forecast_value.values[lat, lon]

    for i in range(len(date_index_sample)):
        for lead in range(9):
            # read in the SMIPS data for the date and save to array
            obs_pre_shuffle[lead, i] = coord_observed[date_index_sample[i] + lead]

    # make and fill smips array
    for lead in range(9):
        fc_to_shuffle = fc_pre_shuffle[lead]
        obs_to_shuffle = obs_pre_shuffle[lead]
        # pass the SMIPS and forecast data arrays to shuffle function

        shuffled_fc = shuffle_random_ties(fc_to_shuffle, obs_to_shuffle)
        # save shuffled_fc to netcdf
        forecast_cube.add_to_netcdf_cube(settings.shuffled_forecast_filename(placeholder_date, lats[lat], lons[lon]),
                                         lead, shuffled_fc)


def create_shuffled_forecasts():
    """
    Reassemble grid: For each grid point: "shuffle" to restore spatial correlations
    Output: shuffled 7 day forecast for grid point
    """
    #date_template = create_date_template()

    # don't actually need dates, but date indices from the smips file. we're only referencing smips
    date_index_sample = source_cube.sample_date_indices()  # need this to be created once and then always be the same

    # for each grid cell
    lats, lons = source_cube.get_lat_lon_values()

    for lat in range(len(lats)):
        if -37.6423 <= lats[lat] <= -27.9675 :
            for lon in range(len(lons)):
                if 140.6947 <= lons[lon] <= 153.7687:
                    shuffle(lat, lon, date_index_sample)

    forecast_cube.aggregate_netcdf(placeholder_date, settings.FORECAST_SHUFFLE_PATH)


def grid_date_forecast(date, lat, lon):
    mu, cov, tp = parameter_cube.read_parameters(lat, lon)
    transform.transform_forecast(lat, lon, date, mu, cov, tp)


def create_forecast_files(date):
    """
    Read the ACCESS-G data for a given forecast date, transform it using the predictor transformation parameters saved
    to netcdf, and save the resulting forecast to netcdf.
    - for all lead times for a date
    - for all grid points
    :return: save the result to a netcdf file
    """
    lats, lons = source_cube.get_lat_lon_values()

    for lat in lats:
        if -37.6423 <= lat <= -27.9675:
            for lon in lons:
                if 140.6947 <= lon <= 153.7687:
                    try:
                        grid_date_forecast(date, lat, lon)
                    except ValueError:
                        continue
    forecast_cube.aggregate_netcdf(date)


def create_parameter_files():
    lats, lons = source_cube.get_lat_lon_values()
    #np.random.seed(50)
    #lat_sample = np.random.choice(lat, y)
    #lon_sample = np.random.choice(lon, y)

    for lat in lats:
        if -37.6423 <= lat <= -27.9675 : #temporary thing bc it was interrupted, to not make it do a whole lot over again
            #if -43.59375 <= lat <= -10.07813: # min/max values where lat stops containing all NaN
            for lon in lons:
                if 140.6947 <= lon <= 153.7687:
                #if 113.2031 <= lon <= 153.6328:
                    parameter_cube.generate_forecast_parameters(lat, lon)
    parameter_cube.aggregate_netcdf()


def daily_jobs():
    """
    Preliminary steps - should be done on a regular automatic basis
    1. Get latest ACCESS-G files from NCI
    2. Regrid latest SMIPS files
    3. Aggregate new ACCESS-G and SMIPS files into ACCESS-G.nc and SMIPS.nc respectively
    """
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    source_cube.aggregate_netcdf(smips=True)
    source_cube.aggregate_netcdf(accessg=True)
    print('Daily jobs done')


if __name__ == '__main__':
    #daily_jobs()
    #create_parameter_files()
    #create_forecast_files(placeholder_date)
    create_shuffled_forecasts()
    forecast_cube.aggregate_netcdf(placeholder_date, settings.FORECAST_SHUFFLE_PATH)

    print('Done NSW')


# def create_date_template():  # not going to use this?
#     # create a date template [9][1000]
#     # fill the 1000 dimension variable with unique possible dates, then in the lead time dimension increment the date by each day
#     datedeltas = source_cube.get_datedeltas(cubepathname=settings.SMIPS_AGG)
#
#     date_sample = random.sample(datedeltas, 1000)
#
#     increment_date = lambda x, y: y + datetime.timedelta(days=1)
#
#     date_template = np.zeros((9, 1000))
#     date_template[0, :] = date_sample
#
#     for i in range(1, 8):
#         date_template[i, :] = increment_date(date_template[i, :], date_template[i - 1, :])
#
#     print(date_template)
#     return date_template