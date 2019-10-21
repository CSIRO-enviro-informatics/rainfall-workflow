import settings
from netCDF4 import Dataset
import os
import xarray as xr
import glob
from dates import get_dates, convert_date
import datetime
import numpy as np

aggregated_smips = 'SMIPS.nc'
aggregated_access_g = 'ACCESS-G.nc'
aggregated_params = 'PARAMS.nc'

smips_name = 'SMIPS'
access_name = 'ACCESS'



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

        if 'SMIPS' in cubepathname:
            refcube = xr.open_dataset(settings.SMIPS_DEST_PATH + settings.smips_filename('20190101'))
            rows = len(refcube.lat.values)
            cols = len(refcube.lon.values)

            outcube.description = 'SMIPS Daily Outputs'
            outcube.history = 'Created ' + datetime.datetime.now().isoformat()

            outcube.createDimension('lon', cols)#cols
            outcube.createDimension('lat', rows)#rows
            xlon = outcube.createVariable('lon', 'f4', 'lon')
            ylat = outcube.createVariable('lat', 'f4', 'lat')

            outcube.createDimension('time', None)#days
            nctime = outcube.createVariable('time', 'u4', 'time')
            nctime.setncatts({"long_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar":"gregorian"})
            nctime[:] = time[:days]

            blended_precipitation = outcube.createVariable('blended_precipitation', 'f', ('time', 'lat', 'lon'), least_significant_digit=3, fill_value=-9999.0)
            blended_precipitation.setncatts({'units':'millimetres'})

            # add attributes
            xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
            ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

            # add lat/lon data
            print(xlon.size, ylat.size, rows, cols)
            xlon[:] = refcube.lon.values
            ylat[:] = refcube.lat.values

        elif 'ACCESS' in cubepathname:
            refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
            rows = len(refcube.lat.values)
            cols = len(refcube.lon.values)

            outcube.Conventions = 'CF-1.5,ACDD-1.3'
            outcube.institution = 'Australian Bureau of Meteorology'
            outcube.source = 'APS2'
            outcube.summary = 'forecast data'
            outcube.title = 'forecast data'
            outcube.base_time = 1200
            outcube.modl_vrsn = 'ACCESS-G'

            outcube.createDimension('lon', cols)  # cols
            outcube.createDimension('lat', rows)  # rows
            xlon = outcube.createVariable('lon', 'f4', 'lon')
            ylat = outcube.createVariable('lat', 'f4', 'lat')

            outcube.createDimension('time', None)  # days
            nctime = outcube.createVariable('time', 'u4', 'time')
            nctime.setncatts(
                {"long_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar": "gregorian"})
            nctime[:] = time[:days]


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

            # add attributes
            xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84", 'axis': 'X'})
            ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84", 'axis': 'Y'})

            # add lat/lon data
            print(xlon.size, ylat.size, rows, cols)
            xlon[:] = refcube.lon.values
            ylat[:] = refcube.lat.values

    elif 'params' in cubepathname:
        # create cube for grid parameters
        # also check paramaters if you're creating a single-grid cube or a whole-grid cube for aggregation

        if 'aggregate' in cubepathname:
            refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
            rows = len(refcube.lat.values)
            cols = len(refcube.lon.values)

            outcube.description = 'Normal and transformed parameters for entire grid.'
            outcube.history = 'Created ' + datetime.datetime.now().isoformat()

            outcube.createDimension('normal_set', 1000)
            outcube.createDimension('transformed_set', 1)
            outcube.createVariable('normal_set', 'u4', 'normal_set')
            outcube.createVariable('transformed_set', 'u4', 'transformed_set')

            outcube.createDimension('normal_parameters', 5)
            outcube.createDimension('transformed_parameters', 4)
            outcube.createVariable('normal_parameters', 'f4', 'normal_parameters')
            outcube.createVariable('transformed_parameters', 'f4', 'transformed_parameters')

            outcube.createDimension('lead_time', 9)
            outcube.createVariable('lead_time', 'u4', 'lead_time')

            outcube.createDimension('lon', cols)  # cols
            outcube.createDimension('lat', rows)  # rows
            ylat = outcube.createVariable('lat', 'f4', 'lat')
            xlon = outcube.createVariable('lon', 'f4', 'lon')

            # add attributes
            xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
            ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

            xlon[:] = refcube.lon.values
            ylat[:] = refcube.lat.values

            # data variables
            outcube.createVariable('normal_param', 'f', ('lat', 'lon', 'lead_time', 'normal_set', 'normal_parameters'), least_significant_digit=3,
                                   fill_value=-9999.0)
            outcube.createVariable('transformed_param', 'f', ('lat', 'lon', 'lead_time', 'transformed_set', 'transformed_parameters'), least_significant_digit=3,
                                   fill_value=-9999.0)

        else:
            if not lat or not lon:
                print('Need to run with lat/lon parameters')

            outcube.description = 'Normal and transformed parameters for grid at: ' + lat + ', ' + lon
            outcube.history = 'Created ' + datetime.datetime.now().isoformat()

            outcube.createDimension('normal_set', 1000)
            outcube.createDimension('transformed_set', 1)
            norm_set = outcube.createVariable('normal_set', 'u4', 'normal_set')
            trans_set = outcube.createVariable('transformed_set', 'u4', 'transformed_set')

            outcube.createDimension('normal_parameters', 5)
            outcube.createDimension('transformed_parameters', 4)
            norm_parameters = outcube.createVariable('normal_parameters', 'c', 'normal_parameters')
            trans_parameters = outcube.createVariable('transformed_parameters', 'c', 'transformed_parameters')

            outcube.createDimension('lead_time', 9)
            lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

            # data variables
            outcube.createVariable('normal_param', 'f', ('lead_time', 'normal_set', 'normal_parameters'), least_significant_digit=3,
                                   fill_value=-9999.0)
            outcube.createVariable('transformed_param', 'f', ('lead_time', 'transformed_set', 'transformed_parameters'), least_significant_digit=3,
                                   fill_value=-9999.0)

            ylat = outcube.createVariable('lat', 'f4')
            xlon = outcube.createVariable('lon', 'f4')

            # add attributes
            xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
            ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

            ylat[:] = lat
            xlon[:] = lon
            norm_set[:] = np.array([x for x in range(1000)])
            trans_set[:] = 0
            norm_parameters[:] = ['u', 'v', 'o', 'p', 'r']
            trans_parameters[:] = ['l', 'e', 'm', 'f']
            lead[:] = np.array([x for x in range(9)])

    outcube.close()

def add_to_netcdf_cube(cubename, normal_data, transformed_data, lead_time):
    """
    Adds params data to a single grid cube.
    :param cubename: name of the cube - will contain 'params', and lat and lon info
    :param normal_data: param data to add; [1000, 5]
    :param transformed_data: param data to add; [4]
    :return: void
    """
    cubepathname = os.path.join(settings.PARAMS_GRIDS_PATH, cubename)
    _, lat, lon = cubename.rstrip('.nc').split('_')
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname, lat=lat, lon=lon)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    norm = outcube.variables['normal_param']
    norm[lead_time, :] = normal_data[:]
    trans = outcube.variables['transformed_param']
    trans[lead_time, :] = [[1, 2, 3, 4]]
    outcube.close()

def add_to_netcdf_cube_from_files(end_date, files, cubename, refresh=True):
    """
    Adds to a netCDF cube SMIPS or ACCESS-G data or params from files - aggregates.
    Parameters:
        cubepathname -- indicates if 'SMIPS' or 'ACCESS' or 'params' - name must contain either of these strings
        enddate -- end date of data todo: make optional
        files -- source of data to add as a list from glob.glob
    """
    if 'SMIPS' in cubename or 'ACCESS' in cubename:
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
            #data = dataset[var_name][:240,:154,:136].values
            if 'SMIPS' in cubename:
                data = dataset[var_name].values
                datain = np.where(data==9.96921e+36, -9999.0, data)

            elif 'ACCESS' in cubename:
                if '20181008' in file:  # file with incomplete lead time dimension
                    padded = np.full((240, 154, 136), 1.0E36)
                    padded[:120, :154, :136] = dataset[var_name][:120, :154, :136].values
                    #data = np.where(padded == 0, -9999.0, padded)
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
    elif 'params' in cubename:
        # add parameter data to aggregate netcdf cube
        _, lat, lon = cubename.rstrip('.nc').split('_')
        cubepathname = os.path.join(settings.PARAMS_PATH, cubename)
        if not os.path.exists(cubepathname):
            print('NetCDF Cube doesn\'t exist at ', cubepathname)
            create_cube(cubepathname)
        outcube = Dataset(cubepathname, mode='a', format='NETCDF4')
        for file2process in files:
            file = file2process
            dataset = xr.open_dataset(file, decode_times=False)
            normal_data = dataset['normal_param'].values
            transformed_data = dataset['transformed_param'].values
            norm_datain = np.where(normal_data == 9.96921e+36, -9999.0, normal_data)
            trans_datain = np.where(transformed_data == 9.96921e+36, -9999.0, transformed_data)

            print('Exporting to netCDF for grid (lat, lon): ', lat, ",", lon)

            norm = outcube.variables['normal_param']
            norm[lat, lon, :] = norm_datain[:]
            trans = outcube.variables['transformed_param']
            trans[lat, lon, :] = [trans_datain[:]]

    outcube.close()

    return True, True


def aggregate_netcdf(update_only=True, start_date=None, end_date=None, smips=False, accessg=False, params=False):

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
        files = settings.params_filename
        nc = xr.open_dataset(path + aggregated_params)
        files = [file for file in glob.glob(path +'*.nc')]
        add_to_netcdf_cube_from_files(cubename=aggregated_params, files=files)


if __name__ == '__main__':
    aggregate_netcdf(smips=True)
    aggregate_netcdf(accessg=True), #start_date=datetime.date(2017, 5, 17), end_date=datetime.date(2017, 5, 18))
