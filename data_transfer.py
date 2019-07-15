# Move ACCESS-G forecast data from NCI to our network
# limit to Australia only to save space
# Have to run through terminal due to getpass()

import pysftp
from getpass import getpass
import datetime
import xarray as xr
import glob
import os
import settings

networkPath = settings.ACCESS_G_PATH


def create_str_date(date):
    str_year = str(date.year)
    str_month = add_missing_zeros([str(date.month)])[0]
    str_day = add_missing_zeros([str(date.day)])[0]
    return str_year + str_month + str_day


def get_dates(start_date=datetime.date(2019, 1, 1), end_date = datetime.date.today()):
    years = [str(x) for x in range(start_date.year, end_date.year+1)]

    num_months = [x for x in range(1,13)]
    num_days = [x for x in range(1,32)]

    #now = datetime.datetime.now()
    #now = datetime.datetime(2019, 1, 4, 12, 00)  # test
    if start_date.year == end_date.year:
        months = [str(x) for x in num_months if start_date.month <= x <= end_date.month]
    else:
        months = [str(x) for x in num_months if start_date.month <= x]
        months.extend([str(x) for x in num_months if x <= end_date.month])
    months = add_missing_zeros(months)

    # Not equal to today as won't have data that recent.
    days = [str(x) for x in num_days]
    days = add_missing_zeros(days)

    dates = []
    for year in years:
        for month in months:
            if int(month) == start_date.month and end_date.month and start_date.year == end_date.year:
                month_dates = [year + month + day for day in days[start_date.day-1:end_date.day-1]]

            elif int(month) == start_date.month and int(year) == start_date.year:
                if month == '02':
                    month_dates = [year + month + day for day in days[start_date.day-1:28]]
                elif month in ['04', '06', '09', '11']:
                    month_dates = [year + month + day for day in days[start_date.day-1:30]]
                else:
                    month_dates = [year + month + day for day in days[start_date.day-1:31]]

            elif int(month) == end_date.month and int(year) == end_date.year:
                month_dates = [year + month + day for day in days[:end_date.day-1]]

            elif year==end_date.year and month < end_date.month:
                get_full_month(year, month, days)

            elif year==start_date.year and month > start_date.month:
                get_full_month(year, month, days)

            else:
                month_dates = []

            dates.extend(month_dates)
    print(dates)
    return dates


def get_full_month(year, month, days):
    if month == '02':
        month_dates = [year + month + day for day in days[:28]]
    elif month in ['04', '06', '09', '11']:
        month_dates = [year + month + day for day in days[:30]]
    else:
        month_dates = [year + month + day for day in days[:31]]
    return month_dates

def add_missing_zeros(str_array):
    return [x if len(x) == 2 else '0' + x for x in str_array]


# Limit coordinates of global netcdf file to Australia
def limit_coordinates(netcdf_file_path):
    data = xr.open_mfdataset(netcdf_file_path)
    # aus_data = data.sel(lat=slice(-9.005, -43.735), lon=slice(112.905, 153.995))  # coordinates matching SMIPS
    aus_data = data.sel(lat=slice(-9.140625, -45.0), lon=slice(110.03906, 157.85156)) # coordinates from past bounded access-g data
    return aus_data


def transfer_files(start_date=None, end_date=datetime.date.today()):
    my_hostname = 'raijin.nci.org.au'
    my_username = 'aa1582'
    my_password = getpass()

    # Connection can't find hostkey
    #cnopts=pysftp.CnOpts()
    #cnopts.hostkeys = None

    if not start_date:
        start_date = get_start_date()

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    if start_date >= today or (start_date == yesterday and datetime.datetime.now().hour < 8):
        # The previous day's 1200 file is uploaded to NCI at ~7.30am each day
        return (print('Files are already up to date'))

    dates = get_dates(start_date, end_date)

    with pysftp.Connection(host=my_hostname, username=my_username, password=my_password) as sftp:
        print("Connection succesfully established ... ")

        # Switch to a remote directory
        sftp.cwd('/g/data3/lb4/ops_aps2/access-g/0001/')

        nc_filename = 'accum_prcp.nc'
        hour = settings.ACCESS_HOUR

        localPath = 'test/'

        for date in dates:
            new_file_name = settings.access_g_filename(date)
            remoteFilePath = date + '/' + hour + '/fc/sfc/' + nc_filename
            localFilePath = localPath + new_file_name
            sftp.get(remoteFilePath, localFilePath)

            australiaFile = limit_coordinates(localFilePath)
            australiaFile.to_netcdf(networkPath + new_file_name)

            print('File: ' + new_file_name + ' written')
    # connection closed automatically at the end of the with-block


# Look into the files at osm to find which recent haven't been uploaded yet
def get_start_date(file_path = networkPath):  # Adapted to be usable for getting regridded smips files
    list_of_files = glob.glob(file_path + '2019/*.nc')
    #print(networkPath, list_of_files)
    latest_file = max(list_of_files, key=os.path.getctime)
    #print(latest_file)
    latest_file = latest_file.rsplit('_')[-1]
    if '12z' in file_path:
        start_date = latest_file.split('12.nc', 1)[0]
    else:
        start_date = latest_file.split('.nc', 1)[0]
    #print(start_date)

    start_year = int(start_date[:4])
    start_month = int(start_date[4:6])
    start_day = int(start_date[6:8])
    #print(start_year, start_month, start_day)
    # Return date of one day after newest file
    return datetime.date(start_year, start_month, start_day) + datetime.timedelta(days=1)


if __name__ == '__main__':
    transfer_files()  # Run without args to only get new files
    #transfer_files(start_date=datetime.date(2017,11,25), end_date=datetime.date(2017,11,26))  # Run with start and end date (not inclusive of end)
    #print(get_dates(get_start_date()))

