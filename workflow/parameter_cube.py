"""\ Functions for managing parameter data and netCDF4 files."""


import os
from netCDF4 import Dataset
import datetime

import xarray as xr
import glob
import numpy as np
import math

from . import settings, bjpmodel, transform
from .source_cube import get_lat_lon_indices, get_real_aps3_lat_lon_values

class ParameterCubeReader(object):
    _file_name = settings.PARAMS_AGG()
    _p = None
    _refs = 0
    _lat_chunks = {}
    __slots__ = tuple()

    def __new__(cls):
        self = super(ParameterCubeReader, cls).__new__(cls)
        if cls._p is None:
            # Use cache=false because we don't want every read variable to accumulate in RAM
            cls._p = xr.open_dataset(cls._file_name, cache=False)
        cls._refs = cls._refs + 1
        return self

    def __del__(self):
        cls = self.__class__
        cls._refs = max(cls._refs - 1, 0)
        if cls._refs < 1 and cls._p is not None:
            try:
                cls._p.close()
            except BaseException:
                pass
            cls._p = None

    def preload_lat_chunk(self, lat):
        try:
            _l = self._lat_chunks[lat]
        except LookupError:
            _l = self._p.sel(lat=lat, method=None)
            # if load_into_ram:
            _l.load()
            self._lat_chunks[lat] = _l
        return _l

    def get_lat_lon(self, lat, lon):
        if lat in self._lat_chunks:
            chunk = self._lat_chunks[lat]
        else:
            chunk = self.preload_lat_chunk(lat)
        return chunk.sel(lon=lon, method=None)

    # def get_lat(self, lat, load_into_ram=False):
    #     try:
    #         _l = self._lat_chunks[lat]
    #     except LookupError:
    #         _l = self._p.sel(lat=lat)
    #         if load_into_ram:
    #             _l.load()
    #         self._lat_chunks[lat] = _l
    #     return _l



def read_parameters(lat, lon, param_cube_slice=None):
    """\
    Read the mu, cov, and transform parameter data from netcdf and reconstruct arrays with the same shape and order as returned from BJP
    sample.
    @return mu[9][1000][2], cov[9][1000][3], tp[9][2][3]
    """
    print("Reading params for lat: {}, lon: {}".format(lat, lon), flush=True)
    if param_cube_slice is None:
        p = ParameterCubeReader()
        p_grid = p.get_lat_lon(lat, lon)
    else:
        p_grid = param_cube_slice.sel(lon=lon)
    if np.isnan(p_grid['n_parameters']).all():
        print("All NANs, skipping.", flush=True)
        raise ValueError('Parameters arrays are empty - write them to file first')

    tp = p_grid['t_parameters'].values
    tpe = np.empty_like(tp)
    tpe[:, 0, 0] = tp[:, 0, 0]
    tpe[:, 1, 0] = tp[:, 1, 0]
    tpe[:, 0, 1] = np.array([math.exp(x) for x in tp[:, 0, 1]])
    tpe[:, 1, 1] = np.array([math.exp(x) for x in tp[:, 1, 1]])
    tpe[:, 0, 2] = np.array([math.exp(x) for x in tp[:, 0, 2]])
    tpe[:, 1, 2] = np.array([math.exp(x) for x in tp[:, 1, 2]])
    nop = p_grid['n_parameters']
    mu = nop[:, :, :2]
    cov = nop[:, :, 2:]
    return mu, cov, tpe


def generate_forecast_parameters(lat, lon, observed_xa, forecast_xa, skip_existing=False):
    """\
    For each grid point: Create post-processed forecast
    1. Extract symmetric grid point(s) from observed and forecast and transform
    2. Model fit/forecast
        - Transform predictor and predictand time series to normal distributions
        - TODO: RPP-SC - call fit() during training , and forecast() during regular use
            - Model parameters from fit() are saved and loaded to forecast()
        -  Output: post-processed 9 day time series forecast for grid point.
            - Dimensions: lead time (9), ensemble member (1000)
    """
    params_file_name = settings.params_filename(lat, lon)
    if skip_existing:
        cubepathname = os.path.join(settings.PARAMS_GRIDS_PATH(), params_file_name)
        if os.path.exists(cubepathname):
            print("parameters exist for {} {}, skipping".format(lat, lon), flush=True)
            return
    print("generating parameters for {} {}".format(lat, lon), flush=True)
    start_date = settings.ACCESS_STARTDATE
    end_date = settings.PARAMS_DATE

    fit_data = transform.extract_fit_data(lat, lon, observed_xa, forecast_xa, start_date, end_date)
   # Don't process the data if a timezone wasn't found because it's in the ocean
    #if fit_data == 'Location over water':
    #    return

    bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=3000, seed='random')

    # Original routine, for reference
    # for lt in range(9):
    #     fdata = np.array([fit_data['ptor'][:, lt], fit_data['ptand'][:, lt] ], order='C')
    #
    #     mu, cov, tparams = bjp_model.sample(fdata)
    #
    #     # Save mu, cov, and transformation parameters to netcdf file for that grid point
    #     normal_params = np.concatenate((mu, np.asarray(cov)), axis=1)
    #     # tparams are from the LogSinhTransformation library. 0,1,2 are Lambda, Epsilon, and ScaleFactor
    #     tparams[0, 1] = math.log(tparams[0, 1])  # save epsilon as a log because it is small - prevent loss of data
    #     tparams[1, 1] = math.log(tparams[1, 1])
    #     tparams[0, 2] = math.log(tparams[0, 2])  # save scaling factor as log for the same reason
    #     tparams[1, 2] = math.log(tparams[1, 2])
    #     add_to_netcdf_cube(params_file_name, lt, normal_params, tparams)
    param_gen = ((np.concatenate((mu, np.asarray(cov)), axis=1), tparams) for (mu, cov, tparams) in (bjp_model.sample(np.array([fit_data['ptor'][:, lt], fit_data['ptand'][:, lt] ], order='C')) for lt in range(9)))
    normal_params = []
    tparams = []
    _ = [normal_params.append(a) or tparams.append(b) for (a, b) in param_gen]
    for t in tparams:
        t[0, 1] = math.log(t[0, 1])  # save epsilon as a log because it is small - prevent loss of data
        t[1, 1] = math.log(t[1, 1])
        t[0, 2] = math.log(t[0, 2])  # save scaling factor as log for the same reason
        t[1, 2] = math.log(t[1, 2])

    add_to_netcdf_cube(params_file_name, range(9), normal_params, tparams)

def create_cube(cubepathname, lat=None, lon=None):
    """\ Create cube for grid parameters"""
    # also check paramaters if you're creating a single-grid cube or a whole-grid cube for aggregation
    # Lat and lon are optional bc not needed for aggregated file
    if os.path.exists(cubepathname):
        os.remove(cubepathname)
    dirname = os.path.dirname(cubepathname)
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)
    outcube = Dataset(cubepathname, mode='w', format='NETCDF4')

    outcube.history = 'Created ' + datetime.datetime.now().isoformat()

    outcube.createDimension('np_set', 2000) # may be 2000
    outcube.createDimension('tp_set', 2)
    np_set = outcube.createVariable('np_set', 'u4', 'np_set')
    tp_set = outcube.createVariable('tp_set', 'u4', 'tp_set')
    np_set.setncatts({"long_name": "normalised parameter set"})
    tp_set.setncatts({"long_name": "transformed parameter set"})

    outcube.createDimension('np_types', 5)
    outcube.createDimension('tp_types', 3)
    np_types = outcube.createVariable('np_types', 'u4', 'np_types')
    tp_types = outcube.createVariable('tp_types', 'u4', 'tp_types')
    np_types.setncatts({"long_name": "normalised parameter types: mu1, mu2, sigma1, sigma2, scaling_factor"})
    tp_types.setncatts({"long_name": "transformed parameter types: lambda, epsilon, scaling_factor"})

    outcube.createDimension('lead_time', 9)
    lead = outcube.createVariable('lead_time', 'u4', 'lead_time')

    lead[:] = range(9)
    np_set[:] = range(2000)
    tp_set[:] = range(2)
    np_types[:] = range(5)
    tp_types[:] = range(3)

    if 'aggregate' in cubepathname:
        #refcube = xr.open_dataset(settings.ACCESS_G_PATH + settings.access_g_filename('20190101'))
        lat, lon = get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)
        # 307x273 for full-size APS3 grid
        # 305x270 if restrict_size is enabled
        rows = len(lat)
        cols = len(lon)

        outcube.description = 'Normal and transformed parameters for entire grid.'

        outcube.createDimension('lon', cols)  # cols
        outcube.createDimension('lat', rows)  # rows
        ylat = outcube.createVariable('lat', 'f4', 'lat')
        xlon = outcube.createVariable('lon', 'f4', 'lon')

        # data variables
        outcube.createVariable('n_parameters', 'f8', ('lat', 'lon', 'lead_time', 'np_set', 'np_types'),
                               least_significant_digit=3, fill_value=-9999.0)
        outcube.createVariable('t_parameters', 'f8', ('lat', 'lon', 'lead_time', 'tp_set', 'tp_types'),
                               least_significant_digit=3, fill_value=-9999.0)

    else:
        if not lat or not lon:
            print('Need to run with lat/lon parameters')

        outcube.description = 'Normal and transformed parameters for grid at: ' + lat + ', ' + lon
        # data variables
        outcube.createVariable('n_parameters', 'f8', ('lead_time', 'np_set', 'np_types'), least_significant_digit=3,
                               fill_value=-9999.0)
        outcube.createVariable('t_parameters', 'f8', ('lead_time', 'tp_set', 'tp_types'), least_significant_digit=3,
                               fill_value=-9999.0)
        ylat = outcube.createVariable('lat', 'f4')
        xlon = outcube.createVariable('lon', 'f4')

    ylat[:] = lat
    xlon[:] = lon

    # add attributes
    xlon.setncatts({"long_name": "longitude", "units": "degrees_east", "proj": "longlat", "datum": "WGS84"})
    ylat.setncatts({"long_name": "latitude", "units": "degrees_north", "proj": "longlat", "datum": "WGS84"})

    outcube.close()


def add_to_netcdf_cube(cubename, lead_time, normal_data, transformed_data):
    """\
        Adds params data to a single grid cube.
        @param cubename: name of the cube - will contain lat and lon info
        @param normal_data: param data to add; [1000, 5]
        @param lead_time: lead time in days; in range(9)
        @param transformed_data: param data to add; [4]
    """

    cubepathname = os.path.join(settings.PARAMS_GRIDS_PATH(), cubename)
    lat, lon = cubename.rstrip('.nc').split(os.path.sep)[-2:]
    if lat.startswith('s'):
        lat = lat.replace('s', '-', 1)
    elif lat.startswith('n'):
        lat = lat.replace('n', '', 1)
    if lon.startswith('w'):
        lon = lon.replace('w', '-', 1)
    elif lon.startswith('e'):
        lon = lon.replace('e', '', 1)
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname, lat=lat, lon=lon)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')
    norm = outcube.variables['n_parameters']
    trans = outcube.variables['t_parameters']
    if isinstance(lead_time, range):
        for i in lead_time:
            norm[i, :] = normal_data[i][:]
            trans[i, :] = transformed_data[i][:]
    else:
        norm[lead_time, :] = normal_data[:]
        trans[lead_time, :] = transformed_data[:]

    outcube.close()


def add_to_netcdf_cube_from_files(files, cubename):
    """\ Add parameter data to aggregate netcdf cube."""
    #cubepathname = os.path.join(settings.PARAMS_PATH, cubename)
    cubepathname = cubename
    if not os.path.exists(cubepathname):
        print('NetCDF Cube doesn\'t exist at ', cubepathname)
        create_cube(cubepathname)
    outcube = Dataset(cubepathname, mode='a', format='NETCDF4')

    lat_indices, lon_indices = get_lat_lon_indices(settings.restrict_size)
    num_files = len(files)
    for i, file2process in enumerate(files):
        file = file2process
        lat, lon = file.rstrip('.nc').split(os.path.sep)[-2:]
        if lat.startswith('s'):
            lat = lat.replace('s','-',1)
        elif lat.startswith('n'):
            lat = lat.replace('n','',1)
        if lon.startswith('w'):
            lon = lon.replace('w','-',1)
        elif lon.startswith('e'):
            lon = lon.replace('e','',1)
        lat, lon = round(float(lat), 3), round(float(lon), 3)  # rounding lat and lon values to sacrifice accuracy for consistency in the dictionary lookup
        print("Loading param file {} of {} for {},{}".format(i+1, num_files, lat, lon), flush=True)
        dataset = xr.open_dataset(file, decode_times=False)
        normal_data = dataset['n_parameters'].values
        transformed_data = dataset['t_parameters'].values
        norm_datain = np.where(normal_data == 9.96921e+36, -9999.0, normal_data)
        trans_datain = np.where(transformed_data == 9.96921e+36, -9999.0, transformed_data)
        dataset.close()
        print("Exporting to netCDF for grid (lat, lon): ", lat, ",", lon, flush=True)
        lat_index = lat_indices[lat]
        lon_index = lon_indices[lon]
        norm = outcube.variables['n_parameters']
        norm[lat_index, lon_index, :] = norm_datain[:]
        trans = outcube.variables['t_parameters']
        trans[lat_index, lon_index, :] = trans_datain[:]
    outcube.close()
    print("Added all files to NetCDF Cube.", flush=True)


def aggregate_netcdf():
    """"\ Aggregate parameter files"""
    path = settings.PARAMS_GRIDS_PATH()
    files = [file for file in glob.glob(path + '*/*.nc', recursive=True)]
    add_to_netcdf_cube_from_files(files, settings.PARAMS_AGG())
