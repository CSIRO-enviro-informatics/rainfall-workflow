import numpy as np
from netCDF4 import Dataset, num2date, date2num, date2index
from datetime import datetime, timedelta
import os
import settings


def add_to_netcdf_cube(date, files, cubename, refresh):
    cubepathname = os.path.join(settings.OUTCUBEPATH,cubename)
    localrefresh = refresh
    cube_new = False
    if not os.path.exists(cubepathname):
        print 'NetCDF Cube doesn\'t exist at ', cubepathname
        create_cube(cubepathname,settings.EFFECTIVESTARTDATE,date,"SMIPS Daily Outputs")
        cube_new = True

    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    time = outcube.get_variables_by_attributes(long_name='time')
    if len(time) == 0:
        print 'error: no time variable found'
        return False, False
    delta = timedelta(int(time[0][0]))
    startdelta = delta.days
    startbase = datetime(1900, 1, 1)
    datedelta = (date - startbase).days
    dateindex = datedelta - startdelta
    start = startbase + delta

    if date < start:
        print 'date is before start date in NetCDF file ', date.isoformat()
        return False, False
    property = files[0][1]
    datalist = outcube.get_variables_by_attributes(long_name=property)

    if len(datalist) == 0:
        localrefresh = True  # time step exists but not the variable
    if datedelta in time[0] and not localrefresh:
        print 'Data for date exists and refresh == False'
        return False, True


    # init lat/lons/time
    lats = np.arange(-9.005, -43.740, -0.01)
    lons = np.arange(112.905, 154.005, 0.01)
    rows = len(lats)
    cols = len(lons)

    for file2process in files:
        file = file2process[0]
        property = file2process[1]
       units = file2process[2]
        datalist = outcube.get_variables_by_attributes(long_name=property)

        if len(datalist) == 0:
            data = outcube.createVariable(property, 'f4', ('time', 'lat', 'lon'), zlib=True, fill_value=-9999.0,
                                          least_significant_digit=3)
            data.setncatts({"long_name": property, "units": units})
        else:
            data = datalist[0]
        #index = len(outcube.dimensions['time'])

        print 'Exporting to netCDF for date: ', date.isoformat()

        # read the flt data
        datain = np.fromfile(file, dtype='f4')
        #datain.fill(0)

        # shape the data into a 2d array, then make a masked array
        dataout = np.reshape(datain, (rows, cols))

        # add time and data to array
        time[0][dateindex] = datedelta
        data[dateindex,:] = dataout[:]

    outcube.close()

    return True, True

def create_cube(cubepathname, startdate, enddate, description):
    #first check output file exits and if so delete
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')
    delta = enddate - startdate
    days = delta.days + 1
    # init lat/lons/time
    lats = np.arange(-9.005, -43.740, -0.01)
    lons = np.arange(112.905, 154.005, 0.01)
    dayssince = (startdate - datetime(1900, 1, 1)).days
    time = np.arange(dayssince,dayssince + days,1)
    rows = len(lats)
    cols = len(lons)

    outcube.description = description
    outcube.history = 'Created ' + datetime.now().isoformat()

    outcube.createDimension('lon', cols)#cols
    outcube.createDimension('lat', rows)#rows
    xlon = outcube.createVariable('lon', 'f4', 'lon')
    ylat = outcube.createVariable('lat', 'f4', 'lat')

    outcube.createDimension('time', None)#days
    nctime = outcube.createVariable('time', 'i4', 'time')
    nctime.setncatts({"long_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar":"gregorian"})
    nctime[:] = time[:days]

    # add attributes
    xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
    ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

    # add lat/lon data
    xlon[:] = lons[:cols]
    ylat[:] = lats[:rows]

    outcube.close()

    return True
