"""\
Regrid SMIPS data using iris library to match ACCESS-G shape.

"""
import pathlib
from functools import partial

import iris
from os import getenv
import xarray as xr
import numpy as np
import rasterio
import datetime
from dateutil.relativedelta import relativedelta
from . import settings
from .dates import date2str, get_dates, check_latest_local_files, convert_date

smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER  # original


def open_smips_file():
    if open_smips_file.cached:
        return open_smips_file.cached
    nc = xr.open_dataset(smips_file)
    open_smips_file.cached = nc
    return nc
    # min_lat = min(smips_nc.lat.values)
    # max_lat = max(smips_nc.lat.values)
    # min_lon = min(smips_nc.lon.values)
    # max_lon = max(smips_nc.lon.values)
open_smips_file.cached = None

def get_smips_nc_blended_precipetation_dateslice(xd, d):
    """\
    Extract and treat each day seperately - for the future, and because the SMIPS file is so big anything else would cause memory errors.
    @param xd: SMIPS netCDF Dataset file opened by xarray
    @param d: date to extract
    @return: iris cube with just blended_precipitation at just that date
    """
    ncp = xd['Blended Precipitation']
    if isinstance(d, datetime.date) and not isinstance(d, datetime.datetime):
        # the new versions of xarray need this to be a datetime, not a date.
        d = datetime.datetime(d.year, d.month, d.day)
    ncpd = ncp.sel(time=d)
    ncpd.name = 'blended_precipitation'
    where_nans = np.isnan(ncpd)
    where_zeros = ncpd == 0.00000
    if np.all(where_nans):
        cube = None
    elif np.all(where_zeros):
        cube = None
    elif np.all(np.logical_or(where_nans, where_zeros)):
        cube = None
    else:
        cube = ncpd.to_iris()
        cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
        cube.coord('latitude').standard_name = 'latitude'
    return cube

def get_smips_ml_precip_dateslice(d, with_NaNs=True):
    USE_COGS = True
    # try:
    #     # read the flt data
    #     d = np.fromfile(origin_path, dtype='f4')
    # except:
    #     return False
    # # init lat/lons
    # lat = np.arange(-9.005, -43.740, -0.01)
    # lon = np.arange(112.905, 154.005, 0.01)
    # rows = len(lat)
    # cols = len(lon)
    # # shape the data into a 2d array, then make a masked array
    # d1 = np.reshape(d, (rows, cols))
    # return d1, lat, lon
    try:
        if USE_COGS:
            origin_path = settings.ml_precip_src_path(d, date_at="end", ext="tif")
            driver = "GTiff"
        else:
            origin_path = settings.ml_precip_src_path(d, date_at="start", ext="flt")
            driver = "EHdr"
        raster = rasterio.open(origin_path, driver=driver)
    except FileNotFoundError:
        return None
    except rasterio.RasterioIOError as e:
        error_str = str(e)
        if "No such file" in error_str:
            return None
        elif "HTTP" in error_str and "404" in error_str:
            return None
        raise
    xa = xr.open_rasterio(raster, parse_coordinates=True)
    #We want just the first band, it is the precip
    xa = xa.isel(band=0)
    if with_NaNs:
        new_vals = np.where(xa == -9999, np.NaN, xa)
        xa.data = new_vals
    where_nans = np.isnan(xa)
    where_zeros = xa == 0.00000
    if np.all(where_nans):
        cube = None
    elif np.all(where_zeros):
        cube = None
    elif np.all(np.logical_or(where_nans, where_zeros)):
        cube = None
    else:
        cube = xa.to_iris()
    cube.coord(var_name='x').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord(var_name='y').standard_name = 'latitude'
    cube.coord(var_name='x').units = 'degrees_east'
    cube.coord(var_name='y').units = 'degrees_north'
    return cube



def save_timestep(cube, str_date):
    """\
    Save the regridded netCDF file containing blended_precipitation data for a date.
    @param cube: netCDF container open as an xarray Dataset
    @param str_date: string representation of a date in the form YYYYMMDD
    """
    new_file = pathlib.Path(settings.SMIPS_DEST_PATH + settings.smips_regrid_dest_filename(str_date))
    new_file.parent.mkdir(parents=True, exist_ok=True)
    xa = xr.DataArray.from_iris(cube)
    #iris.save(cube, str(new_file))
    xd = xa.to_dataset(name="blended_precipitation")
    try:
        xd = xd.drop_dims("band")
    except ValueError:
        pass
    try:
        xd = xd.drop_vars("band")
    except ValueError:
        pass
    xd.attrs['Conventions'] = "CF-1.6"
    xd.to_netcdf(str(new_file))
    print(str(new_file) + ' saved')


def init_regridder():
    """\
    Creating the regridder - most resource intensive part, so do only once.
    Initalised with random SMIPS and ACCESS-G files for reference.
    """

    target_file = settings.ACCESS_G_APS3_PATH + settings.access_g_filename('20200101')  # random APS3 access-g file
    if settings.SMIPS_PRECIP_SOURCE == "SMIPS":
        smips_nc = open_smips_file()
        smips_iris = get_smips_nc_blended_precipetation_dateslice(smips_nc, datetime.date(2020, 1, 1))  # random smips file
        smips_iris.coord('longitude').guess_bounds()
        smips_iris.coord('latitude').guess_bounds()
    elif settings.SMIPS_PRECIP_SOURCE == "blendedPrecipOutputs":
        files_fn = settings.smips_regrid_dest_filename
        smips_nc = open_smips_file()
        smips_iris = get_smips_nc_blended_precipetation_dateslice(smips_nc, datetime.date(2020, 1, 1))  # random smips file
        smips_iris.coord('longitude').guess_bounds()
        smips_iris.coord('latitude').guess_bounds()
    elif settings.SMIPS_PRECIP_SOURCE.startswith("MLPrecip"):
        smips_nc = None
        smips_iris = get_smips_ml_precip_dateslice(datetime.date(2020, 1, 1))
        smips_iris.coord(var_name='y').guess_bounds()  #lat
        smips_iris.coord(var_name='x').guess_bounds()  #lon
    else:
        raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(settings.SMIPS_PRECIP_SOURCE))


    target = iris.load_cube(target_file, 'accumulated precipitation') #, constraint)

    target.coord('longitude').guess_bounds()
    target.coord('latitude').guess_bounds()

    regridder = iris.analysis.AreaWeighted().regridder(smips_iris, target)
    return regridder


def regrid(cube, regridder):
    """\ Regrid a SMIPS cube"""
    cube.coord('longitude').guess_bounds()
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord('latitude').standard_name = 'latitude'
    return regridder(cube)


def run_smips_regridding(update_only=True, start_date=False, end_date=False, skip_existing=True):
    """\
    Re-sample SMIPS geographic grids to match ACCESS-G's.
    @param update_only: if true, only run for new or start_date -> end_date inclusive files. else, run for all files
    @param start_date: file date to start regridding
    @param end_date: file date to end regridding (non inclusive). on any day, can only be as recent as yesterday
    @param skip_existing: don't do regirdding process if regridded file already exists. Aggregation still occurs
    """

    regridder = init_regridder()
    if settings.SMIPS_PRECIP_SOURCE == "SMIPS":
        smips_xd = open_smips_file()
        slice_getter = partial(get_smips_nc_blended_precipetation_dateslice, smips_xd)
    elif settings.SMIPS_PRECIP_SOURCE == "blendedPrecipOutputs":
        smips_xd = open_smips_file()
        slice_getter = partial(get_smips_nc_blended_precipetation_dateslice, smips_xd)
    elif settings.SMIPS_PRECIP_SOURCE.startswith("MLPrecip"):
        smips_xd = open_smips_file()
        slice_getter = get_smips_ml_precip_dateslice
    else:
        raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(settings.SMIPS_PRECIP_SOURCE))
    REGRID_SMIPS_TEST = getenv("REGRID_SMIPS_TEST", "")
    done_dates = []
    if not update_only:
        all_smips_times = smips_xd.time[:]
        for datet in all_smips_times:
            date = convert_date(datet)  # from np datetime to datetime datetime
            str_date = date2str(date)  # for file name
            timestep = get_smips_nc_blended_precipetation_dateslice(smips_xd, date)  # this is the daily smips cube
            if timestep is None:
                # Got a null blended_precipetation. This can happen if the file is new, this will be the last one
                if all_smips_times[-1] == datet:
                    break
                else:
                    raise RuntimeError("Found a null slice in blended_precipetation at {}".format(str_date))
            regridded = regrid(timestep, regridder)  # regridding to match access-g shape
            save_timestep(regridded, str_date)  # save to disk
            done_dates.append(date)
            if REGRID_SMIPS_TEST:
                break
    else:
        d1 = relativedelta(days=1)
        if not start_date:
            (recent_date, recent_file) = check_latest_local_files(settings.SMIPS_DEST_PATH, smips=True)
            if not recent_date:
                start_date = convert_date(smips_xd.time[0])  # Get first date from smips
            else:
                start_date = recent_date.date() + d1  # we want to try to find the day after the latest one
        nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        todayz = nowz.date()
        yesterdayz = todayz - d1
        if start_date >= yesterdayz:
            print('Regrid files are already up to date')
            #print(start_date, yesterday)
            return
        if not end_date:
            end_date = yesterdayz # because SMIPS data is two days behind: eg on 02/07 at 1am, the last point was added, for 30/06
        str_dates = get_dates(start_date, end_date=end_date)
        for str_date in str_dates:
            date = datetime.date(int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8]))
            if skip_existing:
                check_file = pathlib.Path(settings.SMIPS_DEST_PATH + settings.smips_regrid_dest_filename(str_date))
                if check_file.exists():
                    print("skipping regridding smips for {}, file exists at {}".format(str_date, str(check_file)))
                    continue
            print("regridding smips for {}".format(str_date))
            timestep = slice_getter(date)
            if timestep is None:
                # got a null timestep, this can happen on the most recent timestep, if the file is new
                if str_dates[-1] == str_date:
                    break
                else:
                    raise RuntimeError("Found a null slice in SMIPS precipitation source at {}".format(str_date))
            regridded = regrid(timestep, regridder)  # regridding to match access-g shape
            save_timestep(regridded, str_date)  # save to disk
            done_dates.append(date)
            if REGRID_SMIPS_TEST:
                break
    return done_dates

if __name__ == '__main__':
    run_smips_regridding()
