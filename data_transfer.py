# Move ACCESS-G forecast data from NCI to our network
# limit to Australia only to save space
# Have to run through terminal due to getpass()

import pysftp
import datetime
import xarray as xr
import settings
from dates import get_dates, get_start_date

networkPath = settings.ACCESS_G_PATH


def limit_coordinates(netcdf_file_path):
    """Limit coordinates of a global netCDF file to Australia."""
    data = xr.open_dataset(netcdf_file_path)
    # aus_data = data.sel(lat=slice(-9.005, -43.735), lon=slice(112.905, 153.995))  # coordinates matching SMIPS
    aus_data = data.sel(lat=slice(-9.140625, -45.0), lon=slice(110.03906, 157.85156)) # coordinates from past bounded access-g data
    return aus_data


def transfer_files(start_date=None, end_date=datetime.date.today()):
    """
    Transfer daily ACCESS-G files from NCI to network location.
        Need an NCI login and private ssh key with NCI - or, password input.
        If password input, have to run this from the terminal and not with an IDE's "run".

    Parameters:
        start_date -- starting date for files you'll download
        end_date -- end date for files you'll download (not inclusive)

    Run without arguments to update - only transfer files newer than the newest file.
    """
    my_hostname = 'raijin.nci.org.au'
    my_username = 'aa1582'
    #my_password = getpass()
    private_key = '~/.ssh/id_rsa'

    if not start_date:
        start_date = get_start_date(settings.ACCESS_G_PATH)

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    if start_date >= today or (start_date == yesterday and datetime.datetime.now().hour < 8):
        # The previous day's 1200 file is uploaded to NCI at ~7.30am each day
        return (print('ACCESS-G downloaded files are already up to date'))

    dates = get_dates(start_date, end_date)

    with pysftp.Connection(host=my_hostname, username=my_username, private_key=private_key) as sftp:
        print("Connection succesfully established ... ")

        # Switch to a remote directory
        sftp.cwd('/g/data3/lb4/ops_aps2/access-g/0001/')

        nc_filename = 'accum_prcp.nc'
        hour = settings.ACCESS_HOUR

        localPath = 'temp/'

        for date in dates:
            new_file_name = settings.access_g_filename(date)
            remoteFilePath = date + '/' + hour + '/fc/sfc/' + nc_filename
            localFilePath = localPath + new_file_name
            sftp.get(remoteFilePath, localFilePath)

            australiaFile = limit_coordinates(localFilePath)
            australiaFile.to_netcdf(networkPath + new_file_name)

            print('File: ' + new_file_name + ' written')
    # connection closed automatically at the end of the with-block

if __name__ == '__main__':
    transfer_files()

