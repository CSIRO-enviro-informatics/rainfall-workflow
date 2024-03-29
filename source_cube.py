"""!
Functions for managing SMIPS and ACCESS-G data and netCDF4 files. See parameter_cube.py for similar function documentation.
"""

import settings
from netCDF4 import Dataset
import os
import xarray as xr
import glob
from dates import get_dates, convert_date
import datetime
import numpy as np
import random

aggregated_smips = 'SMIPS.nc'
aggregated_access_g = 'ACCESS-G.nc'

smips_name = 'SMIPS'
access_name = 'ACCESS'
day_before_yesterday = datetime.date.today() - datetime.timedelta(days=2)
yesterday = datetime.date.today() - datetime.timedelta(days=1)



def sample_date_indices():
    """!
    Samples 1000 SMIPS dates.
    @return list of date indices
    """
    observed = xr.open_dataset(settings.SMIPS_AGG, decode_times=False)
    max_date_index = len(observed.time.values) - 8 # to ensure we don't get the last value and don't have "lead time" values for it
    date_index_sample = random.sample(range(max_date_index), 1000)
    return date_index_sample


def get_datedeltas(cubepathname=settings.ACCESS_G_AGG, end_date=yesterday):
    """! Gets timedelta representations of the dates of a cube's time dimension."""
    refcube = Dataset(cubepathname, mode='a', format='NETCDF4')
    time = refcube.get_variables_by_attributes(long_name='time')
    if len(time) == 0:
        print('error: no time variable found')
        return False, False
    delta = datetime.timedelta(int(time[0][0]))
    startdelta = delta.days
    startbase = datetime.date(1900, 1, 1)
    datedelta = (end_date - startbase).days

    return range(startdelta, datedelta)


def get_lat_lon_values():
    """! Return lists of latitude and longitude values."""
    refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
    return refcube.lat.values, refcube.lon.values


def get_lat_lon_indices():
    """!
    Return dictionaries of latitude and longitude (value, index) as (key, value) pairs.
    0.2f rounded values are used to avoid key errors.
    """
    lats, lons = get_lat_lon_values()
    lat_indices = {round(float(lat), 2): index for (lat, index) in zip(lats, range(len(lats)))}
    lon_indices = {round(float(lon), 2): index for (lon, index) in zip(lons, range(len(lons)))}
    return lat_indices, lon_indices



def create_cube(cubepathname, startdate=None, enddate=None):
    """!
    Creates a netCDF cube for SMIPS or ACCESS-G aggregated data
    Will delete a cube corresponding to cubepathname if it exists.
    @param cubepathname -- indicates if 'SMIPS' or 'ACCESS' of 'params' - name must contain either of these strings
    @param startdate -- start date of data
    @param enddate -- end date of data
    """
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')

    if 'SMIPS' in cubepathname or 'ACCESS' in cubepathname:
        delta = enddate - startdate
        days = delta.days + 1

        dayssince = (startdate - datetime.datetime(1900, 1, 1)).days
        time = np.arange(dayssince,dayssince + days,1)

        lat, lon = get_lat_lon_values()
        rows = len(lat)
        cols = len(lon)

        outcube.createDimension('lon', cols)
        outcube.createDimension('lat', rows)
        xlon = outcube.createVariable('lon', 'f4', 'lon')
        ylat = outcube.createVariable('lat', 'f4', 'lat')

        outcube.createDimension('time', None)  # days
        nctime = outcube.createVariable('time', 'u4', 'time')
        nctime.setncatts(
            {"long_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar": "gregorian"})
        nctime[:] = time[:days]

        # add attributes
        xlon.setncatts(
            {"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84", 'axis': 'X'})
        ylat.setncatts(
            {"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84", 'axis': 'Y'})

        if 'SMIPS' in cubepathname:
            #refcube = xr.open_dataset(settings.SMIPS_DEST_PATH + settings.smips_filename('20190101'))

            outcube.description = 'SMIPS Daily Outputs'
            outcube.history = 'Created ' + datetime.datetime.now().isoformat()

            blended_precipitation = outcube.createVariable('blended_precipitation', 'f', ('time', 'lat', 'lon'), least_significant_digit=3, fill_value=-9999.0)
            blended_precipitation.setncatts({'units':'millimetres'})

        elif 'ACCESS' in cubepathname:
            #refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))

            outcube.Conventions = 'CF-1.5,ACDD-1.3'
            outcube.institution = 'Australian Bureau of Meteorology'
            outcube.source = 'APS2'
            outcube.summary = 'forecast data'
            outcube.title = 'forecast data'
            outcube.base_time = 1200
            outcube.modl_vrsn = 'ACCESS-G'

            lead_times = [x*3600 for x in range (1, 241)]
            outcube.createDimension('lead_time', 240)
            ncleadtime = outcube.createVariable('lead_time', 'u4', 'lead_time')
            ncleadtime.setncatts(
                {"long_name": "lead_time", "calendar": "gregorian", 'axis':'T','units': 'seconds since created date 12:00:00'})
            ncleadtime[:] = lead_times[:]

            accum_prcp = outcube.createVariable('accum_prcp', 'f', ('time', 'lead_time', 'lat', 'lon'),
                                                           least_significant_digit=3, fill_value=-9999.0)
            accum_prcp.setncatts({'units': 'kg m-2', 'grid_type': 'spatial', 'long_name': 'accumulated precipitation',
                                  'accum_type': 'accumulative', 'accum_units': 'hrs', 'accum_value': 4})#, 'missing_value': 1.0E36})

        # add lat/lon data
        print(xlon.size, ylat.size, rows, cols)
        xlon[:] = lon
        ylat[:] = lat

    outcube.close()


def add_to_netcdf_cube_from_files(files, cubename, refresh=True, end_date=None):
    """!
    Adds to a netCDF cube SMIPS or ACCESS-G data or params from files - aggregates.
    @param cubepathname -- indicates if 'SMIPS' or 'ACCESS' or 'params' - name must contain either of these strings
    @param enddate -- end date of data todo: make optional
    @param files -- source of data to add as a list from glob.glob
    """
    if 'SMIPS' in cubename or 'ACCESS' in cubename:
        if not end_date:
            print('End date is required for SMIPS/ACCESS-G')
        if 'SMIPS' in cubename:
            var_name = 'blended_precipitation'
            cubepathname = os.path.join(settings.SMIPS_DEST_PATH, cubename)
            start_date = settings.SMIPS_STARTDATE
        elif 'ACCESS' in cubename:
            var_name = 'accum_prcp'
            cubepathname = os.path.join(settings.ACCESS_G_PATH,cubename)
            start_date = settings.ACCESS_STARTDATE
        localrefresh = refresh
        if not os.path.exists(cubepathname):
            print ('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname,start_date,end_date)

        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

        time = outcube.get_variables_by_attributes(long_name='time')
        if len(time) == 0:
            print ('error: no time variable found')
            return False, False
        delta = datetime.timedelta(int(time[0][0]))
        startdelta = delta.days
        startbase = datetime.date(1900, 1, 1)
        datedelta = (end_date - startbase).days
        start = startbase + delta

        if end_date < start:
            print ('date is before start date in NetCDF file ', end_date.isoformat())
            return False, False
        property = files[0][1]
        datalist = outcube.get_variables_by_attributes(long_name=property)

        if len(datalist) == 0:
            localrefresh = True  # time step exists but not the variable
        if datedelta in time[0] and not localrefresh:
            print ('Data for date exists and refresh == False')
            return False, True

        if 'ACCESS' in cubename:
            lead_times = [x * 3600 for x in range(1, 241)]

        for file2process in files:
            file = file2process

            dataset = xr.open_dataset(file, decode_times=False)
            if 'SMIPS' in cubename:
                data = dataset[var_name].values
                datain = np.where(data==9.96921e+36, -9999.0, data)

            elif 'ACCESS' in cubename:
                if '20181008' in file:  # file with incomplete lead time dimension
                    padded = np.full((240, 154, 136), 1.0E36)
                    padded[:120, :154, :136] = dataset[var_name][:120, :154, :136].values
                    datain = np.where(padded == 1.0E36, -9999.0, padded)
                else:
                    data = dataset[var_name][:240, :154, :136].values
                    datain = np.where(data==1.0E36, -9999.0, data)
            if 'ACCESS' in cubename:
                str_date = file.rsplit('_', 1)[1].replace('12.nc', "")
            else:
                str_date = file.rsplit('_', 1)[1].replace('.nc', "")
            date = datetime.datetime(int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8]), 12)
            datedelta = (date - datetime.datetime(startbase.year, startbase.month, startbase.day)).days
            dateindex = datedelta - startdelta

            #print('Exporting to netCDF for date: ', date.isoformat())

            var = outcube.variables[var_name]
            var[dateindex, :] = datain[:]
            tme = outcube.variables['time']
            tme[dateindex] = datedelta
            #print(dataset.time.values[dateindex])
            #print(dateindex+1, outcube.variables['time'][dateindex])
            #print(var[dateindex], datain.data[:])

    outcube.close()

    return True, True


def aggregate_netcdf(update_only=True, start_date=None, end_date=None, smips=False, accessg=False):

    if smips or accessg:
        if smips:
            aggregate_file = aggregated_smips
            path = settings.SMIPS_DEST_PATH
            if not end_date:
                end_date = settings.yesterday
            files = settings.smips_filename
        elif accessg:
            aggregate_file = aggregated_access_g
            path = settings.ACCESS_G_PATH
            if not end_date:
                end_date = datetime.date.today()
            files = settings.access_g_filename
        else:
            return print('Run with smips=True or accessg=True')

        if update_only:
            if not start_date:
                if accessg:
                    nc = xr.open_dataset(path + aggregate_file, decode_times=False)
                    latest = nc.time.values[-1]
                    start = datetime.date(1900, 1, 1)
                    start_date = start + datetime.timedelta(int(latest)) + datetime.timedelta(days=1)
                    nc.close()
                    if start_date >= datetime.date.today():
                        return print('ACCESS-G aggregation is already up to date')

                elif smips:
                    nc = xr.open_dataset(path + aggregate_file)
                    latest = nc.time.values[-1]
                    start_date = convert_date(latest) + datetime.timedelta(days=1)
                    nc.close()
                    if start_date >= settings.yesterday:
                        return print('SMIPS aggregation is already up to date')

            dates = get_dates(start_date=start_date, end_date=end_date)
            files = [path + files(date) for date in dates]

        else:
            if smips:
                files = [file for file in glob.glob(path +'*/*.nc')]
            elif accessg:
                files = [file for file in glob.glob(path + '*/*12.nc')]   # there's one file in the access-g directories that's called cdo.nc

        if len(files) <= 0:
            return print('File aggregation is up to date')
        add_to_netcdf_cube_from_files(end_date=end_date, cubename=aggregate_file, files=files)

if __name__ == '__main__':
    aggregate_netcdf(smips=True)
    aggregate_netcdf(accessg=True) #start_date=datetime.date(2017, 5, 17), end_date=datetime.date(2017, 5, 18))
