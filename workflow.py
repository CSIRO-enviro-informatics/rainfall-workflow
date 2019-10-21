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

import argparse
import logging
import os
import sys
from datetime import date

import numpy as np
import matplotlib.pyplot as plt

import data_transfer
import iris_regridding
import cube
import transform
import bjpmodel
from cube import add_to_netcdf_cube

import settings

def daily_jobs():
    """
    Preliminary steps - should be done on a regular automatic basis
    1. Get latest ACCESS-G files from NCI
    2. Regrid latest SMIPS files
    3. Aggregate new ACCESS-G and SMIPS files into ACCESS-G.nc and SMIPS.nc respectively
    """
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    cube.aggregate_netcdf(smips=True)
    cube.aggregate_netcdf(accessg=True)
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

    bjp_model = bjpmodel.BjpModel(2, burn=1000, chainlength=3000, seed='random')

    for lt in range(9):

        fdata = np.array([fit_data['ptor'][:, lt], fit_data['ptand'][:, lt] ], order='C')

        # print(fdata)
        # plt.hist(fdata[0,:], alpha=0.5, label='accessg')
        # plt.hist(fdata[1,:], alpha=0.5, label='smips')
        # plt.legend(loc='best')
        # plt.show()

        mu, cov = bjp_model.sample(fdata, [10, 10])
        # Save mu, cov, and transformation parameters to netcdf file for that grid point
        # Can edit functions in cube.py to accommodate this new kind of file
        normal_params = np.concatenate((mu, np.asarray(cov)), axis=1)
        transformed_params = []
        add_to_netcdf_cube(settings.params_filename(lat, lon), normal_params[:1000], transformed_params, lt)

        #print('lead time', lt, mu.shape, cov.shape, mu[0], np.asarray(cov[0]))



def reassemble():
    """
    Reassemble grid: For each grid point: "shuffle" to restore spatial correlations
    Output: shuffled 7 day forecast for grid point
    """
    print('todo')

if __name__ == '__main__':
    #daily_jobs()
    data_processing(-17.5, 142.0)