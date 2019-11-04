import settings
from netCDF4 import Dataset
import os
import xarray as xr
import glob
from dates import get_dates, convert_date
import datetime
import numpy as np
#from workflow import get_lat_lon_values

aggregated_smips = 'SMIPS.nc'
aggregated_access_g = 'ACCESS-G.nc'
aggregated_params = 'PARAMS_aggregated.nc'
aggregated_fc = 'FORECAST_aggregated.nc'

smips_name = 'SMIPS'
access_name = 'ACCESS'


def get_lat_lon_values():
    refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
    return refcube.lat.values, refcube.lon.values


def create_cube(cubepathname, startdate=None, enddate=None, lat=None, lon=None):
    """
    Creates a netCDF cube for SMIPS or ACCESS-G aggregated data or for single grid or aggregated params data.
    Will delete a cube corresponding to cubepathname if it exists.
    Parameters:
        cubepathname -- indicates if 'SMIPS' or 'ACCESS' of 'params' - name must contain either of these strings
        startdate -- start date of data TODO: make optional
        enddate -- end date of data TODO: make optional
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

    elif 'params' in cubepathname:
        # create cube for grid parameters
        # also check paramaters if you're creating a single-grid cube or a whole-grid cube for aggregation

        outcube.history = 'Created ' + datetime.datetime.now().isoformat()

        outcube.createDimension('np_set', 2000) # may be 2000
        outcube.createDimension('tp_set', 2)
        np_set = outcube.createVariable('np_set', 'u4', 'np_set')
        tp_set = outcube.createVariable('tp_set', 'u4', 'tp_set')
        np_set.setncatts({"long_name": "normalised parameter set"})
        tp_set.setncatts({"long_name": "transformed parameter set"})

        outcube.createDimension('np_types', 5)
        outcube.createDimension('tp_types', 3)
        np_types = outcube.createVariable('np_types', 'u4', 'np_types')
        tp_types = outcube.createVariable('tp_types', 'u4', 'tp_types')
        np_types.setncatts({"long_name": "normalised parameter types: mu1, mu2, sigma1, sigma2, scaling_factor"})
        tp_types.setncatts({"long_name": "transformed parameter types: lambda, epsilon, scaling_factor"})

        outcube.createDimension('lead_time', 9)
        lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

        lead[:] = range(9)
        np_set[:] = range(2000)
        tp_set[:] = range(2)
        np_types[:] = range(5)
        tp_types[:] = range(3)


        if 'aggregate' in cubepathname:
            #refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
            lat, lon = get_lat_lon_values()
            rows = len(lat)
            cols = len(lon)

            outcube.description = 'Normal and transformed parameters for entire grid.'

            outcube.createDimension('lon', cols)  # cols
            outcube.createDimension('lat', rows)  # rows
            ylat = outcube.createVariable('lat', 'f4', 'lat')
            xlon = outcube.createVariable('lon', 'f4', 'lon')

            # data variables
            outcube.createVariable('n_parameters', 'f8', ('lat', 'lon', 'lead_time', 'np_set', 'np_types'),
                                   least_significant_digit=3, fill_value=-9999.0)
            outcube.createVariable('t_parameters', 'f8', ('lat', 'lon', 'lead_time', 'tp_set', 'tp_types'),
                                   least_significant_digit=3, fill_value=-9999.0)

        else:
            if not lat or not lon:
                print('Need to run with lat/lon parameters')

            outcube.description = 'Normal and transformed parameters for grid at: ' + lat + ', ' + lon

            # data variables
            outcube.createVariable('n_parameters', 'f8', ('lead_time', 'np_set', 'np_types'), least_significant_digit=3,
                                   fill_value=-9999.0)
            outcube.createVariable('t_parameters', 'f8', ('lead_time', 'tp_set', 'tp_types'), least_significant_digit=3,
                                   fill_value=-9999.0)

            ylat = outcube.createVariable('lat', 'f4')
            xlon = outcube.createVariable('lon', 'f4')

        ylat[:] = lat
        xlon[:] = lon

        # add attributes
        xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
        ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})



    elif 'forecast' in cubepathname:
        outcube.history = 'Created ' + datetime.datetime.now().isoformat()

        #outcube.createDimension('time', None)  # unlimited
        #nctime = outcube.createVariable('time', 'u4', 'time')
        #nctime.setncatts({"long_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar": "gregorian"})

        outcube.createDimension('ensemble_member', 1000)
        ens = outcube.createVariable('ensemble_member', 'u4', 'ensemble_member')

        outcube.createDimension('lead_time', 9)
        lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

        if 'aggregate' in cubepathname:
            lats, lons = get_lat_lon_values()
            rows = len(lats)
            cols = len(lons)

            outcube.description = 'Normal and transformed parameters for entire grid.'

            outcube.createDimension('lon', cols)  # cols
            outcube.createDimension('lat', rows)  # rows
            ylat = outcube.createVariable('lat', 'f4', 'lat')
            xlon = outcube.createVariable('lon', 'f4', 'lon')

            outcube.createVariable('forecast_value', 'f8', ('lat', 'lon', 'lead_time', 'ensemble_member'), least_significant_digit=3,
                                   fill_value=-9999.0)

            outcube.description = "Post-processed forecast for " + str(startdate)
            ylat[:], xlon[:] = get_lat_lon_values()

        else:
            ylat = outcube.createVariable('lat', 'f4')
            xlon = outcube.createVariable('lon', 'f4')

            outcube.createVariable('forecast_value', 'f8', ('lead_time', 'ensemble_member'), least_significant_digit=3,
                                   fill_value=-9999.0)

            outcube.description = 'Post-processed forecast for grid at: ' + lat + ', ' + lon + " on " + str(startdate)
            ylat[:] = lat
            xlon[:] = lon

        # add attributes
        xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
        ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

        lead[:] = range(9)
        ens[:] = range(1000)

    outcube.close()


def add_to_netcdf_cube(cubename, normal_data, lead_time, transformed_data=None, date=None):
    """
    if params: Adds params data to a single grid cube.
    if forecast: Adds forecast data to a single grid cube
    :param cubename: name of the cube - will contain 'params' or 'forecast', and lat and lon info
    :param normal_data: param data to add; [1000, 5] OR forecast data to add; [1000]
    :param lead_time: lead time in days; in range(9)
    :param transformed_data: param data to add; [4]; params only
    :param date: date for forecast only
    :return: void
    """
    if 'params' in cubename:
        # if transformed_data == None:
        #    print("Run with transformed data parameter")
        #    return
        cubepathname = os.path.join(settings.PARAMS_GRIDS_PATH, cubename)
        _, lat, lon = cubename.rstrip('.nc').split('_')
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname, lat=lat, lon=lon)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

        norm = outcube.variables['n_parameters']
        norm[lead_time, :] = normal_data[:]
        trans = outcube.variables['t_parameters']
        trans[lead_time, :] = transformed_data[:]

    elif 'forecast' in cubename:
        if not date:
            print("Run with date parameter")
            return
        cubepathname = os.path.join(settings.FORECAST_GRID_PATH, cubename)
        _, lat, lon = cubename.rstrip('.nc').split('_')
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname, lat=lat, lon=lon, startdate=date)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

        #tme = outcube.variables['time']
        #startbase = datetime.date(1900, 1, 1)
        #datedelta = (date - startbase).days
        #dateindex = len(tme.values) + 1
        #tme[dateindex] = datedelta
        forecast = outcube.variables['forecast_value']
        forecast[lead_time, :] = normal_data

    outcube.close()

def add_to_netcdf_cube_from_files(files, cubename, refresh=True, end_date=None):
    """
    Adds to a netCDF cube SMIPS or ACCESS-G data or params from files - aggregates.
    Parameters:
        cubepathname -- indicates if 'SMIPS' or 'ACCESS' or 'params' - name must contain either of these strings
        enddate -- end date of data todo: make optional
        files -- source of data to add as a list from glob.glob
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
            if dateindex >= 1203 or not(1 <= dateindex <= 1202):
                print(dateindex, date, file)
                print(datedelta, startbase, startdelta)

            print('Exporting to netCDF for date: ', date.isoformat())

            var = outcube.variables[var_name]
            var[dateindex, :] = datain[:]
            tme = outcube.variables['time']
            tme[dateindex] = datedelta
            #print(dataset.time.values[dateindex])
            print(dateindex+1, outcube.variables['time'][dateindex])
            #print(var[dateindex], datain.data[:])

    elif 'PARAMS' in cubename:
        # add parameter data to aggregate netcdf cube
        cubepathname = os.path.join(settings.PARAMS_PATH, cubename)
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

        lats, lons = get_lat_lon_values()
        lat_indices = {round(float(lat), 2): index for (lat, index) in zip(lats, range(len(lats)))}
        lon_indices = {round(float(lon), 2): index for (lon, index) in zip(lons, range(len(lons)))}

        for file2process in files:
            file = file2process
            _, lat, lon = file.rstrip('.nc').split('_')
            lat, lon = round(float(lat), 2), round(float(lon), 2)  # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup

            dataset = xr.open_dataset(file, decode_times=False)
            normal_data = dataset['n_parameters'].values
            transformed_data = dataset['t_parameters'].values
            norm_datain = np.where(normal_data == 9.96921e+36, -9999.0, normal_data)
            trans_datain = np.where(transformed_data == 9.96921e+36, -9999.0, transformed_data)

            print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)
            lat_index = lat_indices[lat]
            lon_index = lon_indices[lon]
            norm = outcube.variables['n_parameters']
            norm[lat_index, lon_index, :] = norm_datain[:]
            trans = outcube.variables['t_parameters']
            trans[lat_index, lon_index, :] = trans_datain[:]

    elif 'forecast' or 'FORECAST' in cubename:
        # aggregate forecasts
        cubepathname = os.path.join(settings.FORECAST_PATH, cubename)
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname, startdate=end_date)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

        lats, lons = get_lat_lon_values()
        lat_indices = {round(float(lat), 2): index for (lat, index) in zip(lats, range(len(lats)))}
        lon_indices = {round(float(lon), 2): index for (lon, index) in zip(lons, range(len(lons)))}

        for file2process in files:
            file = file2process
            _, lat, lon = file.rstrip('.nc').split('_')
            lat, lon = round(float(lat), 2), round(float(lon), 2)  # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup

            dataset = xr.open_dataset(file, decode_times=False)
            forecast_data = dataset['forecast_value'].values
            fc_datain = np.where(forecast_data == 9.96921e+36, -9999.0, forecast_data)

            print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)
            lat_index = lat_indices[lat]
            lon_index = lon_indices[lon]
            data = outcube.variables['forecast_value']
            data[lat_index, lon_index, :] = fc_datain[:]

    outcube.close()

    return True, True


def aggregate_netcdf(update_only=True, start_date=None, end_date=None, smips=False, accessg=False, params=False, forecast=False):

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
                    if start_date >= settings.yesterday:
                        return print('SMIPS aggregation is already up to date')

                elif smips:
                    nc = xr.open_dataset(path + aggregate_file)
                    latest = nc.time.values[-1]
                    start_date = convert_date(latest) + datetime.timedelta(days=1)
                    nc.close()
                    if start_date >= settings.yesterday:
                        return print('ACCESS-G aggregation is already up to date')

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
    elif params:
        # aggregate parameter files
        path = settings.PARAMS_GRIDS_PATH
        files = [file for file in glob.glob(path +'*.nc')]
        add_to_netcdf_cube_from_files(cubename=aggregated_params, files=files)

    elif forecast:
        path = settings.FORECAST_GRID_PATH
        files = [file for file in glob.glob(path + '*.nc')]
        add_to_netcdf_cube_from_files(cubename=aggregated_fc, files=files, end_date = start_date)


if __name__ == '__main__':
    aggregate_netcdf(smips=True)
    aggregate_netcdf(accessg=True), #start_date=datetime.date(2017, 5, 17), end_date=datetime.date(2017, 5, 18))
