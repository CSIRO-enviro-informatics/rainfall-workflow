# Move ACCESS-G forecast data from NCI to our network
# limit to Australia only to save space

import pysftp
from getpass import getpass
import datetime
import xarray as xr

def get_dates():
    year = '2019'
    num_months = [x for x in range(1,13)]
    num_days = [x for x in range(1,32)]

    now = datetime.datetime.now()
    #now = datetime.datetime(2019, 1, 4, 12, 00)
    months = [str(x) for x in num_months if x <= now.month]
    months = add_missing_zeros(months)

    # Not equal to today as won't have data that recent.
    days = [str(x) for x in num_days]
    days = add_missing_zeros(days)

    dates = []
    for month in months:
        if int(month) != now.month:
            if month == '02':
                month_dates = [year+month+day for day in days[:28]]
            elif month in ['04','06','09','11']:
                month_dates = [year + month + day for day in days[:30]]
            else:
                month_dates = [year + month + day for day in days[:31]]
        else:
            month_dates = [year + month + day for day in days[:now.day-1]]
        dates.extend(month_dates)
    return dates


def add_missing_zeros(str_array):
    return [x if len(x) == 2 else '0' + x for x in str_array]


# Limit coordinates of global netcdf file to Australia
def limit_coordinates(netcdf_file_path):
    data = xr.open_mfdataset(netcdf_file_path)
    aus_data = data.sel(lat=slice(-9.140625, -45.0), lon=slice(110.03906, 157.85156))
    return aus_data


def transfer_past_files():
    my_hostname = 'raijin.nci.org.au'
    my_username = 'aa1582'
    my_password = getpass()

    # Connection can't find hostkey
    #cnopts=pysftp.CnOpts()
    #cnopts.hostkeys = None

    with pysftp.Connection(host=my_hostname, username=my_username, password=my_password) as sftp:
        print("Connection succesfully established ... ")

        # Switch to a remote directory
        sftp.cwd('/g/data3/lb4/ops_aps2/access-g/0001/')

        nc_filename = 'accum_prcp.nc'

        # Define the file that you want to download from the remote directory
        dates = get_dates()
        hour = '1200'
        hr = '12'

        networkPath = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_source/BOM-ACCESS-G/ACCESS_G_00z/2019/'
        localPath = './test/'

        for date in dates:
            new_file_name = 'ACCESS_G_accum_prcp_fc_' + date + hr + '.nc'
            remoteFilePath = date + '/' + hour + '/fc/sfc/' + nc_filename
            localFilePath = localPath + new_file_name
            sftp.get(remoteFilePath, localFilePath)

            australiaFile = limit_coordinates(localFilePath)
            #temp_path = localPath + 'temp_ACCESS_G_accum_prcp_fc_.nc'
            australiaFile.to_netcdf(networkPath + new_file_name)
            #open(networkPath + new_file_name).write(australiaFile)

            print('File: ' + localFilePath + ' written')
    # connection closed automatically at the end of the with-block


if __name__ == '__main__':
    transfer_past_files()
    #print(get_dates())
