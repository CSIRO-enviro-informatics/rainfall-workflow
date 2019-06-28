import os, sys
import calendar

from datetime import datetime

import pytz
import numpy as np
import xarray as xr
import iris

from dateutil.relativedelta import relativedelta

var_name = sys.argv[1]

silo_dir = '/OSM/CBR/LW_HYDROFCT/template/Data/silo/derived/monthly/'
silo_regrid_dir = '/OSM/CBR/LW_HYDROFCT/template/Data/silo/derived/monthly/on_era5_grid/'

min_lat = -45.2
max_lat = -4.8
min_lon = 110.0
max_lon = 155.2

eg_era5_file = '/OSM/CBR/LW_HYDROFCT/template/Data/ecmwf/era5/ensembleDA/derived/monthly_utc/2001/era5_eda_reanalysis_2001_01_monthly_utc_tp.nc'

constraint = iris.Constraint(coord_values={'latitude': lambda cell: min_lat <= cell <= max_lat,
                                           'longitude': lambda cell: min_lon <= cell <= max_lon
                                           })

eg_era5_cube = iris.load_cube(eg_era5_file, constraint)

eg_era5_cube.coord('latitude').guess_bounds()
eg_era5_cube.coord('longitude').guess_bounds()

start_month = datetime(1981, 1, 1)
stop_date = datetime(2017, 12, 1)

for days_shift in [0]:

    target_month = start_month

    while target_month < stop_date:

        new_silo_fname = 'silo.{}.monthly.{}.{}.on.era5.grid.nc'.format(target_month.strftime('%Y_%m'), var_name,
                                                                     'shifted{}days'.format(days_shift)
                                                                     )
        new_silo_fname = os.path.join(silo_regrid_dir, new_silo_fname)

        # if os.path.exists(new_silo_fname):
        #     print(new_silo_fname, 'already exists, skipping')
        #     target_month = target_month + relativedelta(months=1)
        #
        #     continue

        print(target_month)

        orig_silo_fname = 'silo_{}_monthly_{}_{}.nc'.format(target_month.strftime('%Y_%m'), var_name, 'shifted{}days'.format(days_shift))
        orig_silo_fname = os.path.join(silo_dir, orig_silo_fname)

        orig_silo_cube = iris.load_cube(orig_silo_fname)

        orig_silo_cube.coord('latitude').guess_bounds()
        orig_silo_cube.coord('longitude').guess_bounds()

        new_silo_cube = orig_silo_cube.regrid(eg_era5_cube, scheme=iris.analysis.AreaWeighted())

        iris.save(new_silo_cube, new_silo_fname)

        target_month = target_month + relativedelta(months=1)











