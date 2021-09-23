"""\ Functions for managing forecast data and netCDF4 files. See parameter_cube.py for similar function documentation."""

import os
from netCDF4 import Dataset
import datetime
import glob
import xarray as xr
import numpy as np
from . import settings
from .source_cube import get_real_aps3_lat_lon_values, get_lat_lon_indices
from .dates import date2str
from .util import add_crs_var_to_netcdf, crs_wkt_a

# Create a netCDF file for forecasts
# Grid (lat/lon coordinate) or aggregate (containing all coordinates)
# Dimensions:
    # Aggregate: lat (307) x lon (273) x lead_time (9) x ensemble member (1000)
    # Grid: lead_time (9) x ensemble member (1000)
    # Note, this will be 305x270 if using the restrict_size option




def create_cube(cubepathname, date=None, lat=None, lon=None):
    # Lat and lon are optional bc not needed for aggregated file
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    dirname = os.path.dirname(cubepathname)
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)

    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')
    outcube.setncattr("Conventions", "CF-1.6")
    outcube.setncattr("spatial_ref", crs_wkt_a)

    outcube.history = 'Created ' + datetime.datetime.now().isoformat()

    outcube.createDimension('ensemble_member', 1000)
    ens = outcube.createVariable('ensemble_member', 'u4', 'ensemble_member')

    outcube.createDimension('lead_time', 9)
    lead = outcube.createVariable('lead_time', 'u4', 'lead_time')
    lead.setncatts({"long_name": "lead_time", "axis": "T", "standard_name": "time", "units": "days", "calendar": "none"})
    ll_var = add_crs_var_to_netcdf(outcube)
    if 'aggregate' in cubepathname:
        lats, lons = get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)
        rows = len(lats)
        cols = len(lons)

        outcube.createDimension('lon', cols)  # cols
        outcube.createDimension('lat', rows)  # rows
        ylat = outcube.createVariable('lat', 'f4', 'lat')
        xlon = outcube.createVariable('lon', 'f4', 'lon')
        ylat[:] = lats
        xlon[:] = lons

        forecast_var = outcube.createVariable('forecast_value',
                               settings.FORECAST_VALUE_PRECISION,
                               ('ensemble_member', 'lead_time', 'lat', 'lon'),
                               least_significant_digit=3,
                               zlib=False, #Enabling zlib causes write-updates over NFS over the network to take forever
                               fill_value=-9999.0)
        outcube.description = "Post-processed forecast for " + str(date)

    else:
        ylat = outcube.createVariable('lat', 'f4')
        xlon = outcube.createVariable('lon', 'f4')
        ylat[:] = lat
        xlon[:] = lon

        forecast_var = outcube.createVariable('forecast_value',
                               settings.FORECAST_VALUE_PRECISION,
                               ('ensemble_member', 'lead_time'),
                               least_significant_digit=3,
                               zlib=True, #zlib on these is fine because these should only be written to once.
                               fill_value=-9999.0)
        outcube.description = 'Post-processed forecast for grid at: ' + lat + ', ' + lon + " on " + str(date)

    # add attributes
    xlon.setncatts({"long_name": "longitude", "axis": "X", "standard_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
    ylat.setncatts({"long_name": "latitude", "axis": "Y", "standard_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})
    forecast_var.setncatts({"long_name": "forecast_value", "units": "millimeters", "grid_mapping": "crs"})

    lead[:] = range(9)
    ens[:] = range(1000)
    outcube.close()


def add_to_netcdf_cube(cubename, lead_time, data):
    if 'shuffled' in cubename:
        cubepathname = os.path.join(settings.FORECAST_SHUFFLE_PATH(), cubename)
    else:
        cubepathname = os.path.join(settings.FORECAST_GRID_PATH(), cubename)

    datestr, lat, lon = cubename.rstrip('.nc').split('/')[-3:]
    if lat.startswith('s'):
        lat = lat.replace('s', '-', 1)
    elif lat.startswith('n'):
        lat = lat.replace('n', '', 1)
    if lon.startswith('w'):
        lon = lon.replace('w', '-', 1)
    elif lon.startswith('e'):
        lon = lon.replace('e', '', 1)
    datestr = datestr.split("_")[1]
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname, lat=lat, lon=lon)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    forecast = outcube.variables['forecast_value']
    forecast[:, lead_time] = data
    outcube.close()


def add_to_netcdf_cube_from_files(files, cubename, outcube=None):
    # aggregate forecasts
    #cubepathname = os.path.join(settings.FORECAST_PATH, cubename)
    use_multithreading=False
    close_file = False
    if outcube is None:
        cubepathname = cubename
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')
        close_file = True
    #lats, lons = get_lat_lon_values()
    lat_indices, lon_indices = get_lat_lon_indices(settings.restrict_size)
    out_data = outcube.variables['forecast_value']
    def write_out(file2process, lat, lon):
        nonlocal lat_indices, lon_indices, out_data
        dataset = Dataset(file2process, 'r', diskless=True, persist=False)
        #dataset = xr.open_dataset(file2process, decode_cf=False, decode_coords=False, decode_times=False, cache=False)
        #forecast_data = dataset['forecast_value']#.values
        forecast_data = dataset['forecast_value']
        fc_datain = np.where(forecast_data == 9.96921e+36, -9999.0, forecast_data)
        print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)
        lat_index = lat_indices[lat]
        lon_index = lon_indices[lon]
        out_data[:, :, lat_index, lon_index] = fc_datain
        # del fc_datain
        # del forecast_data
        # dataset.close()
    if use_multithreading:
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(processes=8)
        jobs = {}
        for count, file2process in enumerate(files):
            datestr, lat, lon = file2process.rstrip('.nc').split('/')[-3:]
            if lat.startswith('s'):
                lat = lat.replace('s', '-', 1)
            elif lat.startswith('n'):
                lat = lat.replace('n', '', 1)
            if lon.startswith('w'):
                lon = lon.replace('w', '-', 1)
            elif lon.startswith('e'):
                lon = lon.replace('e', '', 1)
            datestr = datestr.split("_")[1]
            # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup
            lat, lon = round(float(lat), 3), round(float(lon), 3)
            job_args = (file2process, lat, lon)
            jobs[file2process] = pool.apply_async(write_out, job_args)
            # if count % 10 == 0:
            #     # Every 10 items, sync to disk
            #     outcube.sync()
        results = [j.get() for f,j in jobs.items()]
    else:
        jobs = {}
        for count, file2process in enumerate(files):
            datestr, lat, lon = file2process.rstrip('.nc').split('/')[-3:]
            if lat.startswith('s'):
                lat = lat.replace('s', '-', 1)
            elif lat.startswith('n'):
                lat = lat.replace('n', '', 1)
            if lon.startswith('w'):
                lon = lon.replace('w', '-', 1)
            elif lon.startswith('e'):
                lon = lon.replace('e', '', 1)
            datestr = datestr.split("_")[1]
            # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup
            lat, lon = round(float(lat), 3), round(float(lon), 3)
            job_args = (file2process, lat, lon)
            jobs[file2process] = job_args
            # if count % 10 == 0:
            #     # Every 10 items, sync to disk
            #     outcube.sync()
        results = [write_out(*args) for f, args in jobs.items()]
    if close_file:
        outcube.close()


def aggregate_netcdf(date, path=None):
    if path is None:
        path = settings.FORECAST_GRID_PATH()
    sdate = date2str(date)
    globpath = settings.shuffled_forecast_dirname(sdate) if 'shuffle' in path else settings.forecast_dirname(sdate)
    files = [file for file in glob.glob(path + globpath + '*/*.nc', recursive=True)]
    ag_name = settings.shuffled_forecast_agg(sdate) if 'shuffle' in path else settings.forecast_agg(sdate)
    create_cube(ag_name, date)
    ds = Dataset(ag_name, mode="a", format='NETCDF4')
    add_to_netcdf_cube_from_files(files, ag_name, outcube=ds)
    ds.close()
