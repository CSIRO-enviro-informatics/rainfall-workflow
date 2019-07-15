# Inputs:
# 	- SMIPS rainfall 1km grid - predictand (DV) - source: CSIRO
# 	- ACCESS-G or -R 12 km grid - predictor (IV) - source: NCI
# 		○ Re-gridded to 1 km grid
#
# Jobs:
# 	- Re-grid SMIPS data from 1km grid to ACCESS-G size grid
#   - Grab the latest matching data ACCESS-G and SMIPS file
# 	- For each grid point: Create post-processed forecast
# 		○ Extract symmetric grid point(s) from predictand and predictor
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
import pytrans
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import datetime
import settings
from data_transfer import create_str_date

import data_transfer
import iris_regridding
import file_management

# import dojobber
# from dojobber import Job
# from dojobber import DummyJob
# from dojobber import RunonlyJob

# Preliminary steps - only need to be checked - should be done on a regular automatic basis?
# 1. Get latest Access-g files
# 2. Regrid latest smips files
# 3. Aggregate new access-g and smips files into the big one

# First step


def transform(data):
    #data = np.random.gamma(3,1, size=500)
    lcens = 0.0
    scale = 5.0/np.max(data)

    transform = pytrans.PyLogSinh(1.0, 1.0, scale)
    transform.optim_params(data, lcens, True, False)
    trans_data = transform.transform_many(transform.rescale_many(data))
    plt.hist(trans_data, bins=50, normed=True, alpha=0.5, label='trans_orig')
    plt.show()
    return trans_data


def transformation():
    # Load data - access/smips
    # Extract 1 grid point of data # data must be in 1D array
    date = datetime.date(2019,6,30)  # Use most recent date

    access_g_file = settings.ACCESS_G_PATH + settings.access_g_filename(create_str_date(date))
    smips_file = settings.smips_filename(create_str_date(date))

    forecast = xr.open_dataset(access_g_file)
    observation = xr.open_dataset(smips_file)

    print(forecast.accum_prcp.values)
    print(observation.blended_precipitation.values)

    # Transform (normalise) a timestep of both observation and forecast data (separately)

    #transform()

def daily_jobs():
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    file_management.aggregate_smips()

if __name__ == '__main__':
    daily_jobs()
