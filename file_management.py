import settings
from netCDF4 import Dataset
import os
import xarray as xr
import glob
from iris_regridding import convert_date
import datetime
import numpy as np

aggregated_smips = 'SMIPS.nc'
aggregated_access_g = 'ACCESS-G.nc'

smips_name = 'SMIPS'
access_name = 'ACCESS'


def create_cube(cubepathname, startdate, enddate):#, description):
    #first check output file exits and if so delete
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')
    delta = enddate - startdate
    days = delta.days + 1

    dayssince = (startdate - datetime.datetime(1900, 1, 1)).days
    time = np.arange(dayssince,dayssince + days,1)

    if 'SMIPS' in cubepathname:
        refcube = xr.open_dataset(settings.smips_filename('20190101'))
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

    outcube.close()

    return True



def add_to_netcdf_cube(date, files, cubename, refresh=True):
    if 'SMIPS' in cubename:
        var_name = 'blended_precipitation'
        cubepathname = os.path.join(settings.SMIPS_PATH, cubename)
        start_date = settings.SMIPS_STARTDATE
    elif 'ACCESS' in cubename:
        var_name = 'accum_prcp'
        cubepathname = os.path.join(settings.ACCESS_G_PATH,cubename)
        start_date = settings.ACCESS_STARTDATE
    localrefresh = refresh
    cube_new = False
    if not os.path.exists(cubepathname):
        print ('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname,start_date,date)
        cube_new = True

    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    time = outcube.get_variables_by_attributes(long_name='time')
    if len(time) == 0:
        print ('error: no time variable found')
        return False, False
    delta = datetime.timedelta(int(time[0][0]))
    startdelta = delta.days
    startbase = datetime.datetime(1900, 1, 1)
    datedelta = (date - startbase).days
    start = startbase + delta

    if date < start:
        print ('date is before start date in NetCDF file ', date.isoformat())
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

        dataset = xr.open_dataset(file)
        #data = dataset[var_name][:240,:154,:136].values
        if 'SMIPS' in cubename:
            data = dataset[var_name].values
            datain = np.where(data==9.96921e+36, -9999.0, data)

        elif 'ACCESS' in cubename:
            if '20181008' in file:  # file with incomplete lead time dimension
                padded = np.zeros((240, 154, 136))
                padded[:120, :154, :136] = dataset[var_name][:120, :154, :136].values
                data = np.where(padded == 0, -9999.0, padded)
                datain = np.where(data == 1.0E36, -9999.0, data)
            else:
                data = dataset[var_name][:240, :154, :136].values
                datain = np.where(data==1.0E36, -9999.0, data)

        str_date = file.rsplit('_', 1)[1].replace('12.nc', "")
        date = datetime.datetime(int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8]), 12)
        datedelta = (date - startbase).days
        dateindex = datedelta - startdelta

        print('Exporting to netCDF for date: ', date.isoformat())

        var = outcube.variables[var_name]
        var[dateindex, :] = datain[:]
        print(dateindex+1)
        #print(var[dateindex], datain.data[:])

    outcube.close()

    return True, True


def aggregate_access_g(year):
    aggregate_file = aggregated_access_g
    #years = ['2016', '2017', '2018', '2019']
    #for year in years:
    files = [filename for filename in glob.glob(settings.ACCESS_G_PATH + str(year) + '/*12.nc')]
    add_to_netcdf_cube(date=datetime.datetime(2019, 6, 30), cubename=aggregate_file, files=files)
    #add_to_netcdf_cube(date=datetime.datetime(2019, 6, 30), cubename=aggregate_file, files=['//OSM/CBR/LW_SATSOILMOIST/source/BOM-ACCESS-G/ACCESS_G_12z/2018/ACCESS_G_accum_prcp_fc_2018103112.nc'])


def aggregate_smips():
    aggregate_file = aggregated_smips
    files = [filename for filename in glob.glob('test/*/*.nc')]
    add_to_netcdf_cube(date=datetime.datetime(2019, 6, 30), cubename=aggregate_file, files=files)


if __name__ == '__main__':
    #aggregate_smips()
    aggregate_access_g(2018)  # will have to re-run later - right now doesn't work because 2018-10-08 file is lead_time-incomplete
    # Have aggregated 2016, 2017, 2019
