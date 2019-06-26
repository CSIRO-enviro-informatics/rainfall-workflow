# Move ACCESS-G forecast data from NCI to our network
# limit to Australia only to save space

import pysftp
from getpass import getpass
import datetime
import xarray as xr
import glob
import os

networkPath = '//osm-12-cdc.it.csiro.au/OSM_CBR_LW_SATSOILMOIST_source/BOM-ACCESS-G/ACCESS_G_00z/2019/'


def get_dates(start_date=datetime.date(2019, 1, 1)):
    year = start_date.year
    num_months = [x for x in range(1,13)]
    num_days = [x for x in range(1,32)]

    now = datetime.datetime.now()
    #now = datetime.datetime(2019, 1, 4, 12, 00)  # test
    months = [str(x) for x in num_months if start_date.month <= x <= now.month]
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
            month_dates = [str(year) + month + day for day in days[start_date.day-1:now.day-1]]
        dates.extend(month_dates)
    print(dates)
    return dates


def add_missing_zeros(str_array):
    return [x if len(x) == 2 else '0' + x for x in str_array]


# Limit coordinates of global netcdf file to Australia
def limit_coordinates(netcdf_file_path):
    data = xr.open_mfdataset(netcdf_file_path)
    # aus_data = data.sel(lat=slice(-9.005, -43.735), lon=slice(112.905, 153.995))  # coordinates matching SMIPS
    aus_data = data.sel(lat=slice(-9.140625, -45.0), lon=slice(110.03906, 157.85156)) # coordinates from past bounded access-g data
    return aus_data


def transfer_files(update_only=True):
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

        if update_only:
            start_date = get_start_date()
            dates = get_dates(start_date)
        else:
            dates=get_dates()

        hour = '1200'
        hr = hour[:2]

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


# Look into the files at osm to find which recent haven't been uploaded yet
def get_start_date():
    list_of_files = glob.glob(networkPath + '*.nc')
    latest_file = max(list_of_files, key=os.path.getctime)
    #print(latest_file)
    latest_file = latest_file.rsplit('_')[-1]
    start_date = latest_file.split('12.nc', 1)[0]
    #print(start_date)

    start_year = int(start_date[:4])
    start_month = int(start_date[4:6])
    start_day = int(start_date[6:8])
    #print(start_year, start_month, start_day)
    # Return date of one day after newest file
    return datetime.date(start_year, start_month, start_day) + datetime.timedelta(days=1)


if __name__ == '__main__':
    transfer_files()  # Run without args to only get new files
    #print(get_dates(get_start_date()))

