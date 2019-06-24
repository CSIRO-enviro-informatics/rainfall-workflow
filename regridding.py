# SMIPS data has to be re-sized to match ACCESS-G data
# SMIPS: ~ 0.89 - 1.2km grid
# ACCESS-G: ~722 - 1001km grid
# see data investigation ipynbs

# Create the target grid description file

import xarray as xr

input_path = '\\\\osm-12-cdc.it.csiro.au\\OSM_CBR_LW_SATSOILMOIST_source\\BOM-ACCESS-G\\ACCESS_G_00z\\2019\\'
input_file = 'ACCESS_G_accum_prcp_fc_2019062112.nc' # random recent ACCESS-G file
output_file = 'target_grid_description.txt'

data = xr.open_mfdataset(input_path+input_file)
y = data.lat.values # latitude = y
x = data.lon.values # longitude = x

#Attributes needed for description file
gridtype = 'curvilinear'
nvertex = 4  # assumed 4 for curvilinear grids
xsize = len(x)
ysize = len(y)
gridsize = xsize * ysize
xvals = x
yvals = y
# TODO: need to find xbounds and ybounds - the lons/lats which surround each grid
#xbounds = # amount will equal to nvertex * gridsize
#ybounds =

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