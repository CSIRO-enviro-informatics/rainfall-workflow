# SMIPS data has to be re-sized to match ACCESS-G data
# SMIPS: ~ 0.89 - 1.2km grid
# ACCESS-G: ~722 - 1001km grid
# see data investigation ipynbs

# Create the target grid description file

import xarray as xr
from nco import Nco
import os
import settings

input_path = settings.ACCESS_G_PATH
input_file = 'ACCESS_G_accum_prcp_fc_2019062112.nc'  # random recent ACCESS-G file

smips_path = settings.SMIPS_PATH
smips_file = settings.SMIPS_CONTAINER

def create_description_file(input_file, output_file):
    data = xr.open_mfdataset(input_path + input_file)
    y = data.lat.values  # latitude = y
    x = data.lon.values  # longitude = x

    # Attributes needed for description file
    gridtype = 'curvilinear'
    nvertex = 4  # assumed 4 for curvilinear grids
    xsize = len(x)
    ysize = len(y)
    gridsize = xsize * ysize
    xvals = x
    yvals = y
    # TODO: need to find xbounds and ybounds - the lons/lats which surround each grid
    # xbounds = # amount will equal to nvertex * gridsize
    # ybounds =

    file = open(output_file, 'w')
    # TODO: include writing xbounds and ybounds after their respective vals
    # possible TODO: get rid of brackets surrounding vals arrays? check if they're an issue
    file.write('gridtype = ' + gridtype +
                '\ngridsize = ' + str(gridsize) +
                '\nxsize = ' + str(xsize) +
                '\nysize = ' + str(ysize) +
                '\nxvals = ' + str(xvals) +
                '\nyvals = ' + str(yvals))
    file.close()


if __name__ == '__main__':
    #output_file = 'target_grid_description.txt'
    #create_description_file(input_file, output_file)

    print(os.environ['PATH'])
    print(os.environ['NCOpath'])

    nco = Nco(debug=True)
    small_smips_file = 'smips_sample.nc'
    # Split off a piece of the smips file to test
    #nco.ncks(input=smips_path+smips_file, output=small_smips_file, options=['-d time,0,1'])  # 1 day

    # Remap
    access_g_file = input_path+input_file
    resample_file = 'resampled_smips_sample.nc'
    nco.ncremap(input=small_smips_file, destination=access_g_file, output=resample_file)

