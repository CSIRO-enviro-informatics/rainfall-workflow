"""\
Functions for managing SMIPS and ACCESS-G data and netCDF4 files. See parameter_cube.py for similar function documentation.
"""
from functools import lru_cache
import os
import xarray as xr
import glob
import datetime
import numpy as np
import random
from dateutil.relativedelta import relativedelta

from .util import crs_wkt_a, add_crs_var_to_netcdf

os.putenv('HDF5_USE_FILE_LOCKING', 'FALSE')
from netCDF4 import Dataset
from . import settings
from .dates import get_dates, convert_date
smips_name = 'SMIPS'
access_name = 'ACCESS'
day_before_yesterday = datetime.date.today() - datetime.timedelta(days=2)
yesterday = datetime.date.today() - datetime.timedelta(days=1)

class SourceCubeReader(object):
    _accessg_file_name = settings.ACCESS_G_AGG
    _smips_file_name = settings.SMIPS_REGRID_AGG()
    _accessg_p = None
    _smips_p = None
    _refs_access_g = 0
    _refs_smips = 0
    _smips_lat_chunks = {}
    _accessg_lat_chunks = {}
    __slots__ = ("which",)

    def __new__(cls, which="accessg"):
        self = super(SourceCubeReader, cls).__new__(cls)
        self.which = str(which).lower()
        if self.which == "smips":
            if cls._smips_p is None:
                # Use cache=false because we don't want every read variable to accumulate in RAM
                cls._smips_p = xr.open_dataset(cls._smips_file_name, decode_times=False, cache=False)
            cls._refs_smips = cls._refs_smips + 1
        elif self.which == "accessg":
            if cls._accessg_p is None:
                # Use cache=false because we don't want every read variable to accumulate in RAM
                cls._accessg_p = xr.open_dataset(cls._accessg_file_name, decode_times=False, cache=False)
            cls._refs_access_g = cls._refs_access_g + 1
        else:
            raise RuntimeError("Bad SourceCube Reader!")
        return self

    def __del__(self):
        cls = self.__class__
        if self.which == "smips":
            cls._refs_smips = max(cls._refs_smips - 1, 0)
            if cls._refs_smips < 1 and cls._smips_p is not None:
                try:
                    cls._smips_p.close()
                except BaseException:
                    pass
                cls._smips_p = None
        elif self.which == "accessg":
            cls._refs_access_g = max(cls._refs_access_g - 1, 0)
            if cls._refs_access_g < 1 and cls._accessg_p is not None:
                try:
                    cls._accessg_p.close()
                except BaseException:
                    pass
                cls._accessg_p = None
        else:
            raise RuntimeError("Bad SouceCube Reader!")

    def preload_lat_chunk(self, lat, sel_date=None):
        if self.which == "smips":
            try:
                _l = self._smips_lat_chunks[(sel_date, lat)]
            except LookupError:
                _l = self._smips_p.blended_precipitation.sel(lat=lat, method=None)
                if sel_date is not None:
                    _l = _l.sel(time=sel_date)
                # if load_into_ram:
                _l.load()
                self._smips_lat_chunks[(sel_date, lat)] = _l
        elif self.which == "accessg":
            date_days = (sel_date - datetime.date(1900, 1, 1)).days
            try:
                _l = self._accessg_lat_chunks[(sel_date, lat)]
            except LookupError:
                _l = self._accessg_p.sel(lat=lat, method=None)
                if sel_date is not None:
                    _l = _l.sel(time=date_days)
                # if load_into_ram:
                _l.load()
                self._accessg_lat_chunks[(sel_date, lat)] = _l
        else:
            raise RuntimeError("Bad SouceCube Reader!")
        return _l

    def get_lat_lon(self, lat, lon, sel_date=None):
        if self.which == "smips" and (sel_date, lat) in self._smips_lat_chunks:
            chunk = self._smips_lat_chunks[(sel_date, lat)]
        elif self.which == "accessg" and (sel_date, lat) in self._accessg_lat_chunks:
            chunk = self._accessg_lat_chunks[(sel_date, lat)]
        else:
            chunk = self.preload_lat_chunk(lat, sel_date=sel_date)
        return chunk.sel(lon=lon, method=None)

def sample_date_indices():
    """\
    Samples 1000 SMIPS dates.
    @return list of date indices
    """
    agg_file = settings.SMIPS_REGRID_AGG()
    observed = xr.open_dataset(str(agg_file), decode_times=False)
    max_date_index = len(observed.time.values) - 8  # to ensure we don't get the last value and don't have "lead time" values for it
    date_index_sample = random.sample(range(max_date_index), 1000)
    return date_index_sample


def get_datedeltas(cubepathname=settings.ACCESS_G_AGG, end_date=yesterday):
    """\ Gets timedelta representations of the dates of a cube's time dimension."""
    refcube = Dataset(cubepathname, mode='a', format='NETCDF4')
    time = refcube.get_variables_by_attributes(long_name='time')
    if len(time) == 0:
        print('error: no time variable found')
        return False, False
    time = time[0]
    delta = datetime.timedelta(int(time[0]))
    startdelta = delta.days
    startbase = datetime.date(1900, 1, 1)
    datedelta = (end_date - startbase).days

    return range(startdelta, datedelta)


@lru_cache()
def get_real_aps3_lat_lon_values(restrict_size=False):
    """\ Return lists of latitude and longitude values."""
    refcube = xr.open_dataset(settings.ACCESS_G_APS3_PATH + settings.access_g_filename('20200101'))
    lats = refcube.lat.values
    lons = refcube.lon.values
    refcube.close()
    if restrict_size:
        # cut down the real values, drop off the leftmost longitude and _two_ rightmost longitudes
        # also drop of the top and bottom latitudes
        lons = lons[1:-2]
        lats = lats[1:-1]

    return lats, lons


def get_lat_lon_indices(restrict_size=False):
    """\
    Return dictionaries of latitude and longitude (value, index) as (key, value) pairs.
    0.3f rounded values are used to avoid key errors.
    """
    lats, lons = get_real_aps3_lat_lon_values(restrict_size=restrict_size)
    lat_indices = {round(float(lat), 3): index for (lat, index) in zip(lats, range(len(lats)))}
    lon_indices = {round(float(lon), 3): index for (lon, index) in zip(lons, range(len(lons)))}
    return lat_indices, lon_indices



def create_cube(cubepathname, startdate=None, enddate=None):
    """\
    Creates a netCDF cube for SMIPS or ACCESS-G aggregated data
    Will delete a cube corresponding to cubepathname if it exists.
    @param cubepathname -- indicates if 'SMIPS' or 'ACCESS' of 'params' - name must contain either of these strings
    @param startdate -- start date of data
    @param enddate -- end date of data
    """
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    if 'SMIPS' not in cubepathname and 'ACCESS' not in cubepathname:
        raise RuntimeError("SMIPS or ACCESS must be in cubepathname")
    if isinstance(startdate, datetime.datetime):
        startdate = startdate.date()
    if isinstance(enddate, datetime.datetime):
        enddate = enddate.date()
    delta = enddate - startdate
    days = delta.days + 1
    epoch_date = datetime.datetime(1900, 1, 1).date()
    dayssince = (startdate - epoch_date).days
    time = np.arange(dayssince,dayssince + days,1)

    lat, lon = get_real_aps3_lat_lon_values()
    rows = len(lat)
    cols = len(lon)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')
    outcube.createDimension('lon', cols)
    outcube.createDimension('lat', rows)
    outcube.setncattr("spatial_ref", crs_wkt_a)
    crs = add_crs_var_to_netcdf(outcube)
    xlon = outcube.createVariable('lon', 'f4', 'lon')
    ylat = outcube.createVariable('lat', 'f4', 'lat')

    outcube.createDimension('time', None)  # days
    nctime = outcube.createVariable('time', 'u4', 'time')
    nctime.setncatts(
        {"long_name": "time", "standard_name": "time", "units": "days since 1900-01-01 00:00:00.0 -0:00", "calendar": "gregorian", "axis": "T"})
    nctime[:] = time[:days]

    # add attributes
    xlon.setncatts(
        {"long_name": "longitude", "standard_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84", 'axis': 'X'})
    ylat.setncatts(
        {"long_name": "latitude", "standard_name": "longitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84", 'axis': 'Y'})
    if 'SMIPS' in cubepathname:
        #refcube = xr.open_dataset(settings.SMIPS_DEST_PATH + settings.smips_filename('20190101'))
        outcube.Conventions = 'CF-1.6'
        outcube.description = 'SMIPS Daily Blended Rainfall'
        outcube.history = 'Created ' + datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

        blended_precipitation = outcube.createVariable('blended_precipitation', settings.SMIPS_PRECIP_VALUE_PRECISION,
                                                       ('time', 'lat', 'lon'), least_significant_digit=3, fill_value=-9999.0)
        blended_precipitation.setncatts({'units': 'millimetres', "long_name": "blended_precipitation", "grid_mapping": "crs"})

    elif 'ACCESS' in cubepathname:
        #refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))

        outcube.Conventions = 'CF-1.6,ACDD-1.3'
        outcube.institution = 'Australian Bureau of Meteorology'
        outcube.source = 'APS3'
        outcube.summary = 'forecast data'
        outcube.title = 'forecast data'
        outcube.base_time = 1200
        outcube.modl_vrsn = 'ACCESS-G'
        hr = 3600
        lead_times = np.arange(1*hr, 241*hr, hr, 'u4') #[x*3600 for x in range (1, 241)]
        outcube.createDimension('lead_time', 240)
        ncleadtime = outcube.createVariable('lead_time', 'u4', 'lead_time')
        ncleadtime.setncatts(
            {"long_name": "lead_time", "calendar": "gregorian", 'axis':'T', 'units': 'seconds since created date 12:00:00'})
        ncleadtime[:] = lead_times[:]
        # TODO, change F8 to a configuratble float size, and re-aggregate all ACCESS-G
        accum_prcp = outcube.createVariable('accum_prcp', 'f8', ('time', 'lead_time', 'lat', 'lon'),
                                                       least_significant_digit=3, fill_value=-9999.0)
        accum_prcp.setncatts({'units': 'kg m-2', 'grid_type': 'spatial', 'long_name': 'accumulated precipitation', "grid_mapping": "crs",
                              'accum_type': 'accumulative', 'accum_units': 'hrs', 'accum_value': 4})#, 'missing_value': 1.0E36})

    # add lat/lon data
    assert (xlon.size, ylat.size) == (cols, rows)
    xlon[:] = lon
    ylat[:] = lat

    return outcube


def add_to_netcdf_cube_from_files(files, cubename, refresh=True, end_date=None):
    """\
    Adds to a netCDF cube SMIPS or ACCESS-G data or params from files - aggregates.
    @param cubepathname -- indicates if 'SMIPS' or 'ACCESS' or 'params' - name must contain either of these strings
    @param enddate -- end date of data todo: make optional
    @param files -- source of data to add as a list from glob.glob
    """
    if 'SMIPS' not in cubename and 'ACCESS' not in cubename:
        raise RuntimeError("Must be SMIPS or ACCESS when operating on cube.")

    if not end_date:
        print('End date is required for SMIPS/ACCESS-G')
    if 'SMIPS' in cubename:
        var_name = 'blended_precipitation'
        cubepathname = os.path.join(settings.SMIPS_DEST_PATH, cubename)
        start_date = settings.SMIPS_STARTDATE
    else:
        # ACCESS-G in cubename
        var_name = 'accum_prcp'
        cubepathname = settings.ACCESS_G_AGG
        start_date = settings.ACCESS_STARTDATE
    localrefresh = refresh
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        outcube = create_cube(cubepathname, start_date, end_date)
    else:
        try:
            outcube = Dataset(cubepathname, mode='a', format='NETCDF4')
        except OSError as e:
            print(e, flush=True)
            raise Exception("\nFile error. Please repair or delete {} and try again.".format(cubepathname))

    time = outcube.get_variables_by_attributes(long_name='time')
    if len(time) == 0:
        print('error: no time variable found')
        return False, False
    time = time[0]
    delta = datetime.timedelta(int(time[0]))  # day as seen in the outcube, is number of days since 1900/01/01
    startdelta = delta.days  # days as integer
    startbase = datetime.date(1900, 1, 1)  # date at 1900/01/01
    enddatedelta = (end_date - startbase).days  # same as startdelta, but for end_date
    start_date = startbase + delta  # actual date of startdelta, same as 1900/01/01 + startdelta

    if end_date < start_date:
        print ('date is before start date in NetCDF file ', end_date.isoformat())
        return False, False

    datalist = outcube[var_name]

    if len(datalist) == 0:
        localrefresh = True  # time step exists but not the variable
    if enddatedelta in time and not localrefresh:
        print('Data for date exists and refresh == False')
        return False, True

    if 'ACCESS' in cubename:
        lead_times = [x * 3600 for x in range(1, 241)]
    try:
        for file2process in files:
            file = file2process
            try:
                dataset = xr.open_dataset(file, decode_times=False)
            except FileNotFoundError as e:
                print(e, flush=True)
                if files[-1] == file:
                    print("Latest file not found. Maybe it wasn't downloaded properly. Skipping.", flush=True)
                    continue
                else:
                    raise RuntimeError(
                        "Cannot put file in the aggregate file, because it was not found:\n{}".format(file))

            if 'SMIPS' in cubename:
                if var_name not in dataset.variables:
                    lowers = {str(v).lower().replace(" ", "_"): v for v in dataset.variables}
                    if var_name in lowers:
                        use_var_name = lowers[var_name]
                    else:
                        raise RuntimeError("Source file {} does not have a variable named {}".format(file, var_name))
                else:
                    use_var_name = var_name
                data = dataset[use_var_name][:307, :273].values
                datain = np.where(data==9.96921e+36, -9999.0, data)

            elif 'ACCESS' in cubename:
                # if '20181008' in file:  # file with incomplete lead time dimension
                #     padded = np.full((240, 307, 273), 1.0E36)
                #     padded[:120, :154, :136] = dataset[var_name][:120, :154, :136].values
                #     datain = np.where(padded == 1.0E36, -9999.0, padded)
                # else:
                data = dataset[var_name][:240, :307, :273].values
                datain = np.where(data==1.0E36, -9999.0, data)
                has_nan = np.any(np.isnan(datain))
                if has_nan:
                    print(has_nan)
            if 'ACCESS' in cubename:
                str_date = file.rsplit('_', 1)[1].replace('12.nc', "")
            else:
                str_date = file.rsplit('_', 1)[1].replace('.nc', "")
            date = datetime.datetime(int(str_date[:4]), int(str_date[4:6]), int(str_date[6:8]), 12)
            thisdatedelta = (date - datetime.datetime(startbase.year, startbase.month, startbase.day)).days
            dateindex = thisdatedelta - startdelta

            #print('Exporting to netCDF for date: ', date.isoformat())

            var = outcube.variables[var_name]
            var[dateindex, :] = datain[:]
            tme = outcube.variables['time']
            tme[dateindex] = thisdatedelta
            #print(dataset.time.values[dateindex])
            #print(dateindex+1, outcube.variables['time'][dateindex])
            #print(var[dateindex], datain.data[:])
    finally:
        outcube.close()

    return True, True


def aggregate_netcdf(update_only=True, start_date=None, end_date=None, smips=False, accessg=False, just_dates=None):

    if not smips and not accessg:
        return

    nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    todayz = nowz.date()
    yesterdayz = todayz - datetime.timedelta(days=1)
    if smips:
        aggregate_file = settings.SMIPS_REGRID_AGG()
        path = settings.SMIPS_DEST_PATH
        if not end_date:
            end_date = yesterdayz
        if settings.SMIPS_PRECIP_SOURCE == "SMIPS":
            files_fn = settings.smips_regrid_dest_filename
        elif settings.SMIPS_PRECIP_SOURCE == "blendedPrecipOutputs":
            files_fn = settings.smips_regrid_dest_filename
        elif settings.SMIPS_PRECIP_SOURCE.startswith("MLPrecip"):
            files_fn = settings.ml_precip_regrid_dest_filename
        else:
            raise RuntimeError("Bad SMIPS_PRECIP_SOURCE: {}".format(settings.SMIPS_PRECIP_SOURCE))
    elif accessg:
        aggregate_file = settings.ACCESS_G_AGG
        path = settings.ACCESS_G_APS3_PATH
        if not end_date:
            end_date = todayz
        files_fn = settings.access_g_filename
    else:
        return print('Run with smips=True or accessg=True')
    files = []
    d1 = relativedelta(days=1)
    if just_dates is not None:
        if not update_only:
            raise RuntimeError("You cannot use just_dates if update_only is False")
        if isinstance(just_dates, list) and len(just_dates) < 1:
            return print("Nothing to aggregate, no new files processed.")
        start_date = just_dates[0]
        end_date = just_dates[-1]
        files = [path + files_fn(date) for date in just_dates]
    elif update_only:
        if not start_date:
            if accessg:
                nc = None
                try:
                    nc = xr.open_dataset(str(aggregate_file), decode_times=False)
                    latest = nc.time.values[-1]
                    start = datetime.date(1900, 1, 1)
                    start_date = start + datetime.timedelta(int(latest)) + d1
                except (FileNotFoundError, AttributeError, LookupError):
                    start_date = None
                finally:
                    if nc:
                        nc.close()
                if start_date and start_date >= todayz:
                    return print('ACCESS-G aggregation is already up to date')

            elif smips:
                nc = None
                try:
                    nc = xr.open_dataset(str(aggregate_file))
                    latest = nc.time.values[-1]
                    start_date = convert_date(latest) + d1
                except (FileNotFoundError, AttributeError, LookupError):
                    start_date = None
                finally:
                    if nc:
                        nc.close()
                if start_date and start_date >= yesterdayz:
                    return print('SMIPS aggregation is already up to date')
        if start_date and not just_dates:
            dates = get_dates(start_date=start_date, end_date=end_date)
            files = [path + files_fn(date) for date in dates]
        else:
            update_only = False
    if not update_only:
        if smips:
            files = settings.find_smips_dest_files(path)
        elif accessg:
            files_old = [file for file in glob.glob(settings.ACCESS_G_REGRIDDED_APS2_PATH + '*/*12.nc')]
            files_new = [file for file in glob.glob(path + '*/*12.nc')]
            files = sorted(files_old + files_new)

    if len(files) <= 0:
        return print('File aggregation is up to date')
    if accessg:
        print("Updating ACCESS_G source cube with start_date={} and end_date={} and files:\n{}".format(start_date, end_date, str(files)), flush=True)
    elif smips:
        print("Updating SMIPS source cube with start_date={} and end_date={} and files:\n{}".format(start_date, end_date, files), flush=True)
    add_to_netcdf_cube_from_files(end_date=end_date, cubename=str(aggregate_file), files=files)

def try_fill_cube_missing_dates(smips=False, accessg=False, check_nans=True, from_date=None):
    nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    todayz = nowz.date()
    yesterdayz = todayz - datetime.timedelta(days=1)
    if smips:
        raise RuntimeError("I don't know how ot fill missing dates in smips cube for now!")
        aggregate_file = settings.SMIPS_REGRID_AGG()
        end_date = yesterdayz
        var = "blended_rainfall"
    elif accessg:
        aggregate_file = settings.ACCESS_G_AGG
        end_date = todayz
        var = "accum_prcp"
    else:
        return False
    nc = xr.open_dataset(str(aggregate_file), decode_times=False)
    offset = datetime.date(1900, 1, 1)
    times = nc.time.values
    if accessg:
        times = [offset + datetime.timedelta(days=int(d)) for d in times]
    not_in_cube = []
    ref = times[0]
    if from_date is None:
        from_date = times[0]
        start_checking = True
    else:
        start_checking = False
    for t in times:
        if not start_checking:
            if t >= from_date:
                from_date = t
                start_checking = True
            else:
                ref = ref + datetime.timedelta(days=1)
                continue
        if t != ref:
            delta = (t - ref)
            print("Missing delta: {}", delta)
            not_in_cube.append(ref)
            ref = t
        else:
            print("Found {}".format(ref))
        ref = ref + datetime.timedelta(days=1)
    if start_checking is False:
        raise RuntimeError("Start date {} not found in the cube.", from_date)
    if from_date != times[0]:
        start_checking = False
    if check_nans:
        for t in times:
            if not start_checking:
                if t >= from_date:
                    start_checking = True
                else:
                    continue
            print("Checking for missing data in {}".format(t))
            if accessg:
                tsel = (t - offset).days
            else:
                tsel = t
            da = getattr(nc, var)
            slice = da.sel(time=tsel)
            if np.all(np.isnan(slice)):
                not_in_cube.append(t)

    print(not_in_cube)
if __name__ == '__main__':
    try_fill_cube_missing_dates(accessg=True)
    aggregate_netcdf(smips=True)
    aggregate_netcdf(accessg=True) #start_date=datetime.date(2017, 5, 17), end_date=datetime.date(2017, 5, 18))
