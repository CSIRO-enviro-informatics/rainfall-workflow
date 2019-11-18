"""! Temporarily used functions to deal with SMIPS error data."""

import xarray as xr
import settings
import datetime
from source_cube import get_lat_lon_indices
import iris_regridding
import iris
from netCDF4 import Dataset
import source_cube

bad_date = datetime.date(2018, 5, 19)


def create_new_file():
    regridder = iris_regridding.init_regridder()
    smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER  # original
    smips_nc = xr.open_dataset(smips_file)

    timestep = iris_regridding.extract_timestep(smips_nc, bad_date)  # this is the daily smips cube
    regridded = iris_regridding.regrid(timestep, regridder)  # regridding to match access-g shape
    iris_regridding.save_timestep(regridded, settings.date_type_check(bad_date))  # save to disk


# change bad data in a smips file 2018-05-19
def edit_netcdf():
    fname = settings.SMIPS_DEST_PATH + settings.smips_filename(bad_date)

    startlat = -34.69
    endlat = -37.73 #.5
    startlon = 146.95
    endlon = 150.12 #-149.7656
    lats, lons = get_lat_lon_indices()

    cube = Dataset(fname, mode='a', format='NETCDF4')
    prcp = cube.variables['blended_precipitation']
    #prcp._FillValue=-9999.0
    prcp[lats[startlat]:lats[endlat], lons[startlon]:lons[endlon]] = -9999.0
    cube.close()


if __name__ == '__main__':
    #create_new_file()
    edit_netcdf()
    source_cube.aggregate_netcdf(smips=True, update_only=False)