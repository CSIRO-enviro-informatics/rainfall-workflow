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
# 				□ Dimensions: time (1), lead time (7), ensemble member (1000)
# 	- Reassemble grid
# 	- For each grid point: restore spatial correlations
# 		○ Shuffle - restore spatial correlations
# 			§ Output: shuffled 7 day forecast for grid point
# 	- Hydrological model
# 		○ Output: 7 day ensemble soil moisture forecast (API)

from datetime import date, timedelta

import numpy as np
import matplotlib.pyplot as plt
import xarray as xr

import data_transfer
import iris_regridding
import transform
import bjpmodel
from cube import add_to_netcdf_cube, aggregate_netcdf
import settings
import pytrans
import math

def daily_jobs():
    """
    Preliminary steps - should be done on a regular automatic basis
    1. Get latest ACCESS-G files from NCI
    2. Regrid latest SMIPS files
    3. Aggregate new ACCESS-G and SMIPS files into ACCESS-G.nc and SMIPS.nc respectively
    """
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    aggregate_netcdf(smips=True)
    aggregate_netcdf(accessg=True)
    print('Daily jobs done')


def data_processing(lat, lon):
    """
    For each grid point: Create post-processed forecast
    1. Extract symmetric grid point(s) from observed and forecast and transform
    2. Model fit/forecast
        - Transform predictor and predictand time series to normal distributions
        - TODO: RPP-SC - call fit() during training , and forecast() during regular use
            - Model parameters from fit() are saved and loaded to forecast()
        - TODO: Output: post-processed 7 day time series forecast for grid point.
            - Dimensions: time (1), lead time (7), ensemble member (1000)
    """

    start_date = settings.ACCESS_STARTDATE
    end_date = date(2019, 8, 1)
    fit_data = transform.extract_fit_data(lat, lon, start_date, end_date)

   # Don't process the data if a timezone wasn't found because it's in the ocean
    if fit_data == 'Location over water':
        return

    bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=3000, seed='random')

    for lt in range(9):

        fdata = np.array([fit_data['ptor'][:, lt], fit_data['ptand'][:, lt] ], order='C')

        # print(fdata)
        # plt.hist(fdata[0,:], alpha=0.5, label='accessg')
        # plt.hist(fdata[1,:], alpha=0.5, label='smips')
        # plt.legend(loc='best')
        # plt.show()

        mu, cov, tparams = bjp_model.sample(fdata)
        #mu, cov = bjp_model.sample(fdata, [10, 10])

        # Save mu, cov, and transformation parameters to netcdf file for that grid point
        # Can edit functions in cube.py to accommodate this new kind of file
        normal_params = np.concatenate((mu, np.asarray(cov)), axis=1)
        tparams[0, 1] = math.log(tparams[0, 1])
        tparams[1, 1] = math.log(tparams[1, 1])

        add_to_netcdf_cube(settings.params_filename(lat, lon), normal_data=normal_params, transformed_data=tparams, lead_time=lt)

        #print('lead time', lt, mu.shape, cov.shape, mu[0], np.asarray(cov[0]))



def reassemble():
    """
    Reassemble grid: For each grid point: "shuffle" to restore spatial correlations
    Output: shuffled 7 day forecast for grid point
    """
    print('todo')


def get_lat_lon_values():
    refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
    return refcube.lat.values, refcube.lon.values


def read_params(file, lat, lon):
    """
    Read the mu and cov data from agg netcdf and reconstruct arrays with the same shape and order as returned from BJP
    sample. Both [1000][2]
    :return:
    """
    p = xr.open_dataset(file)
    p_grid = p.sel(lat=lat, lon=lon)
    if np.isnan(p_grid['n_parameters']).all():
        return [0], [0]

    tp = p_grid['t_parameters']
    nop = p_grid['n_parameters']

    tp[:, 0, 1] = [math.exp(x) for x in tp[:, 0, 1].values]
    tp[:, 1, 1] = [math.exp(x) for x in tp[:, 1, 1].values]

    return nop, tp

def transform_forecast(d):
    """
    Read the ACCESS-G data for a given forecast date and transform it using the predictor transformation parameters saved to netcdf
    - using the "one" set of parameters
    - for all lead times for a date
    - for all grid points at once? I suppose this is what the realistic final function for this will look like, not a
    step in the fitting process. yes, going by the diagram, fit gets me the parameters, forecast lets me use them
    - but the "post processed 7 day time series forecast" says it's for one grid point. one date, one grid point at a time?
    i guess this is the reference data that will be updated every day? for the soil moisture api. keeping the most current day's value
    because the diagram says the time dimension is 1. unless...unlimited?
    whichever way, i gotta make a new type of netcdf file and then an aggregated version of it
    those functions for doing that are getting big. break them down into for specific file types?
    :return: save the result to a netcdf file
    """

    # read an access-g date
    #fc_file = settings.ACCESS_G_AGG
    #fc = xr.open_dataset(fc_file, decode_times=False)
    #datedelta = (d - date(1900, 1, 1)).days
    #data = fc.sel(time=datedelta)

    # read a sample grid point transform parameter set
    p_file = settings.PARAMS_AGG
    lats, lons = get_lat_lon_values()

    for lat in lats:
        if lat == -19.21875:

            for lon in lons:
                if lon == 123.046875:
                    nop, tp = read_params(p_file, lat, lon)
                    if nop.shape == (1,) or tp.shape == (1,):
                        continue

                    for lt in range(9):
                        predictor_tp = tp[lt][0]
                        predictand_tp = tp[lt][1]

                        # mu and cov
                        mu = nop[lt, :, :2]
                        cov = nop[lt, :, 2:]
                        data = transform.extract_data(lat, lon, d, lt)

                        # create a pylogsinh object and then call forecast with it and the t_parameters
                        bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=2000, seed='random')

                        # transformers
                        ptort = pytrans.PyLogSinh(predictor_tp[0], predictor_tp[1], predictor_tp[2])
                        ptandt = pytrans.PyLogSinh(predictand_tp[0], predictand_tp[1], predictand_tp[2])

                        res = bjp_model.forecast([data], [ptort, ptandt], mu.data, cov.data)
                        fc = res['forecast'][:, 1]
                        # the returned trans data is your forecast
                        add_to_netcdf_cube(settings.forecast_filename(lat, lon), normal_data=fc, lead_time=lt, date=d)

    aggregate_netcdf(forecast=True)


def create_grid_param_files():
    lats, lons = get_lat_lon_values()
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
                    data_processing(lat, lon)


if __name__ == '__main__':
    #daily_jobs()
    #create_grid_param_files()
    data_processing(-19.21875, 123.046875)
    aggregate_netcdf(params=True)
    transform_forecast(date(2019, 1, 1)) #-19.21875, 123.046875