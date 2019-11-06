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



def shuffle():
    """
    Reassemble grid: For each grid point: "shuffle" to restore spatial correlations
    Output: shuffled 7 day forecast for grid point
    """
    # create a date template [9][1000]
        # fill the 1000 dimension variable with unique possible dates, then in the lead time dimension increment the date by each day
    datedeltas = source_cube.get_datedeltas()
    date_sample = random.sample(datedeltas, 1000)

    # open big smips file
    # open big forecast file

    # for each grid cell
    lats, lons = source_cube.get_lat_lon_values()
    for lat in lats:
        for lon in lons:
            # make arrays based on datetemplate to save date data
            for date in datedeltas:
                # read in the SMIPS data for the date and save to array
                # also read in the forecast data for that date and save to array

            # pass the SMIPS and forecast data arrays to shuffle function

    print('todo')


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
        for lon in lons:
            try:
                grid_date_forecast(date, lat, lon)
            except ValueError:
                continue


def create_parameter_files():
    lats, lons = source_cube.get_lat_lon_values()
    #np.random.seed(50)
    #lat_sample = np.random.choice(lat, y)
    #lon_sample = np.random.choice(lon, y)

    for lat in lats:
        #if lat == -19.21875:
        #if lat < -18.984375: #temporary thing bc it was interrupted, to not make it do a whole lot over again
        if -43.59375 <= lat <= -10.07813: # min/max values where lat stops containing all NaN
            for lon in lons:
                    #if lon == 123.046875:
                if 113.2031 <= lon <= 153.6328:
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
    daily_jobs()
    #create_parameter_files()
    create_forecast_files(datetime.date(2019, 1, 1))
