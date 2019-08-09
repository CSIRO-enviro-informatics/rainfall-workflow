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
import data_transfer
import iris_regridding
import cube
import transform


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


def data_processing():
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
    transform.transformation()


def reassemble():
    """
    Reassemble grid: For each grid point: "shuffle" to restore spatial correlations
    Output: shuffled 7 day forecast for grid point
    """
    print('todo')

if __name__ == '__main__':
    daily_jobs()
    data_processing()