# Extract data from a NetCDF container
# variable: Blended_Precipitation

import pysftp
from getpass import getpass

my_hostname = 'raijin.nci.org.au'
my_username = 'aa1582'
my_password = getpass()

# Connection can't find hostkey
cnopts=pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(host=my_hostname, username=my_username, password=my_password, cnopts=cnopts) as sftp:
    print("Connection succesfully established ... ")

    # Switch to a remote directory
    sftp.cwd('/g/data3/lb4/ops_aps2/access-g/0001/')

    # Obtain structure of the remote directory '/var/www/vhosts'
    directory_structure = sftp.listdir_attr()

    # Print data
    for attr in directory_structure:
        print(attr.filename, attr)

    nc_filename = 'accum_prcp.nc'

    # Define the file that you want to download from the remote directory
    date = '20190619'
    hour = '1800'
    remoteFilePath = date + '/' + hour + '/fc/sfc/' + nc_filename

    # Define the local path where the file will be saved
    # or absolute "C:\Users\sdkca\Desktop\TUTORIAL.txt"
    localFilePath = '\\\\osm-12-cdc.it.csiro.au\\OSM_CBR_LW_SATSOILMOIST_source\\BOM-ACCESS-G\\ACCESS_G_00z\\2019'

    sftp.get(remoteFilePath, localFilePath)

# connection closed automatically at the end of the with-block

