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

# # Aggregates netcdf files into one container
# def aggregate_files(name):
#     smips, accessg = False, False
#     if name == smips_name:
#         smips = True
#         aggregate_file = aggregated_smips
#     elif name == access_name:
#         accessg = True
#         aggregate_file = aggregated_access_g
#     else:
#         return 'Use with a valid name: {0} or {1}'.format(smips_name, access_name)
#     # Check if aggregated file exists
#     if not os.path.exists(aggregate_file):
#         # If not, create it, utilising files we already have
#         aggregated = Dataset(aggregate_file, "w", format='NETCDF4')
#         if smips:
#             old_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER
#         if accessg:
#             old_file = settings.ACCESS_G_PATH + settings.access_g_filename('20190101')
#
#
#         time = aggregated.createDimension('time', 0)  # 0 makes it an unlimited dimension
#         lat = aggregated.createDimension('lat', 154)
#         lon = aggregated.createDimension('lon', 136)
#         # With extra dimension in the case of access-g.
#         if accessg:
#             lead_time = aggregated.createDimension('lead_time', 240)
#
#         # Create variables - exclude deprecated
#         #if smips:
#         #    longitudes = aggregated.createVariable('lon', )
#         deprecated = ['base_date', 'base_time', 'forc_minutes', 'valid_date', 'valid_time', 'wrtn_date', 'wrtn_time']
#         for vname, var in Dataset(old_file).variables.items():
#             if vname not in deprecated:
#                 x = aggregated.createVariable(vname, var.datatype, var.dimensions)
#                 aggregated[x][:] = Dataset(old_file)[x][:]
#         if accessg:
#             aggregated.renameVariable('time', 'lead_time')
#             aggregated.createVariable('time') # TODO: find out datatype, dimensions=time
#     # Get path to all small files and loop to append them to big one
#         # In case access-g: rename native time dimension to lead time, make the time dimension for the big file = creation date at 12:00
#         # SMIPS can just be appended together


def create_cube(cubepathname, startdate, enddate):#, description):
    #first check output file exits and if so delete
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')
    refcube = xr.open_dataset(settings.smips_filename('20190101'))
    delta = enddate - startdate
    days = delta.days + 1

    dayssince = (startdate - datetime.datetime(1900, 1, 1)).days
    time = np.arange(dayssince,dayssince + days,1)
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

    outcube.close()

    return True

def aggregate_smips():
    aggregate_file = aggregated_smips
    files = [filename for filename in glob.glob('test/*/*.nc')]
    add_to_netcdf_cube(date=datetime.datetime(2019, 6, 30), cubename=aggregate_file, files=files)



def add_to_netcdf_cube(date, files, cubename, refresh=True):
    cubepathname = os.path.join(settings.OUTCUBEPATH,cubename)
    localrefresh = refresh
    cube_new = False
    #if not os.path.exists(cubepathname):
        #print ('NetCDF Cube doesn\'t exist at ', cubepathname)
    create_cube(cubepathname,settings.EFFECTIVESTARTDATE,date)
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


    for file2process in files:
        file = file2process

        dataset = xr.open_dataset(file)
        data = dataset['blended_precipitation'].values
        datain = np.where(data==9.96921e+36, -9999.0, data)

        dt = convert_date(dataset.time.values)
        date = datetime.datetime.combine(dt, datetime.datetime.min.time())
        datedelta = (date - startbase).days
        dateindex = datedelta - startdelta

        print('Exporting to netCDF for date: ', date.isoformat())
        var = outcube.variables['blended_precipitation']
        var[dateindex, :] = datain[:]

        print(dateindex)
        #print(var[dateindex], datain.data[:])

    outcube.close()

    return True, True


if __name__ == '__main__':
    aggregate_smips()
    #aggregate_files(smips_name)
    #aggregate_files(access_name)