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
import data_transfer
import iris_regridding
import cube
import transform

# import dojobber
# from dojobber import Job
# from dojobber import DummyJob
# from dojobber import RunonlyJob

# Preliminary steps - only need to be checked - should be done on a regular automatic basis?
# 1. Get latest Access-g files
# 2. Regrid latest smips files
# 3. Aggregate new access-g and smips files into the big one

# First step
# 1. Load data -access-g/smips
# 2. Extract 1 geographical grid point
# 3. Transform (normalise) SMIPS time series of rainfall on the grid point




def daily_jobs():
    data_transfer.transfer_files()
    iris_regridding.run_regridding()
    cube.aggregate_netcdf(smips=True)
    cube.aggregate_netcdf(accessg=True)
    print('All done')

if __name__ == '__main__':
    daily_jobs()
    transform.transformation()
