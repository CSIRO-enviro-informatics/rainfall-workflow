"""


"""
import iris
import settings
import xarray as xr
import datetime
from dates import date2str, get_dates, get_start_date, convert_date

smips_file = settings.SMIPS_PATH + settings.SMIPS_CONTAINER  # original
smips_nc = xr.open_dataset(smips_file)
min_lat = min(smips_nc.lat.values)
max_lat = max(smips_nc.lat.values)
min_lon = min(smips_nc.lon.values)
max_lon = max(smips_nc.lon.values)


def extract_timestep(nc, date):
    """
    Extract and treat each day seperately - for the future, and because the SMIPS file is so big anything else would cause memory errors.
    Parameters:
        nc -- netCDF file
        date -- date to extract
    """
    ncp = nc['Blended Precipitation']
    ncpd = ncp.sel(time=date)
    ncpd.name = 'blended_precipitation'
    cube = ncpd.to_iris()
    return cube


def save_timestep(cube, str_date):
    """
    Save the regridded netCDF file containing blended_precipitation data for a date.
    Parameters:
        cube -- netCDF container open as an xarray Dataset
        str_date -- string representation of a date in the form YYYYMMDD
    """
    new_file = settings.SMIPS_DEST_PATH + '{}/SMIPS_blnd_prcp_regrid_{}.nc'.format(str_date[:4], str_date)
    iris.save(cube, new_file)
    print(new_file + ' saved')


def init_regridder():
    """
    Creating the regridder - most resource intensive part, so do only once.
    Initalised with random SMIPS and ACCESS-G files.
    """
    target_file = settings.ACCESS_G_PATH + settings.access_g_filename('20190101') # random access-g file
    cube = extract_timestep(smips_nc, datetime.date(2019, 1, 1))  # random smips file

    target = iris.load_cube(target_file, 'accumulated precipitation') #, constraint)

    target.coord('longitude').guess_bounds()
    target.coord('latitude').guess_bounds()

    cube.coord('longitude').guess_bounds()
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord('latitude').standard_name = 'latitude'

    regridder = iris.analysis.AreaWeighted().regridder(cube, target)
    return regridder


def regrid(cube, regridder):
    """ Regrid a SMIPS cube"""
    cube.coord('longitude').guess_bounds()
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').standard_name = 'longitude'  # necessary to guess the coordinate axis
    cube.coord('latitude').standard_name = 'latitude'
    return regridder(cube)


def run_regridding(update_only=True, start_date=False, end_date=False):
    """
    Re-sample SMIPS geographic grids to match ACCESS-G's.
    Parameters:
        update_only -- if true, only run for new or start_date -> end_date inclusive files. else, run for all files
        start_date -- file date to start regridding
        end_date -- file date to end regridding (non inclusive). on any day, can only be as recent as yesterday
    """

    regridder = init_regridder()
    if not update_only:
        for date in smips_nc.time:
            date = convert_date(date)  # from np datetime to datetime datetime
            str_date = date2str(date)  # for file name
            timestep = extract_timestep(smips_nc, date)  # this is the daily smips cube
            regridded = regrid(timestep, regridder)  # regridding to match access-g shape
            save_timestep(regridded, str_date)  # save to disk
    else:
        if not start_date:
           start_date = get_start_date(settings.SMIPS_DEST_PATH)
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        if start_date >= yesterday:
            print('Regrid files are already up to date')
            #print(start_date, yesterday)
            return
        if not end_date:
            end_date = yesterday
        str_dates = get_dates(start_date, end_date=end_date)  # because SMIPS data is two days behind: eg on 02/07 at 1am, the last point was added, for 30/06
        for str_date in str_dates:
            date = datetime.date(int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8]))
            timestep = extract_timestep(smips_nc, date)  # this is the daily smips cube
            regridded = regrid(timestep, regridder)  # regridding to match access-g shape
            save_timestep(regridded, str_date)  # save to disk

if __name__ == '__main__':
    run_regridding()