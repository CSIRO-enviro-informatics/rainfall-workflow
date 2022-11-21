"""\

Main file. Contains highest level functions to execute major jobs.

Inputs:
    - SMIPS rainfall 1km grid - predictand (DV) - source: CSIRO
    - ACCESS-G or -R ~25x39 km grid - predictor (IV) - source: NCI

Jobs:
    - Re-grid SMIPS data from 1km grid to ACCESS-G size grids
  - Grab the latest matching data ACCESS-G and SMIPS file
    - For each grid point: Create post-processed forecast
        ○ Extract symmetric grid point(s) from predictand and predictor and transform
        ○ Model fit/forecast
            § Transform predictor and predictand time series to normal distributions
            § RPP-SC - call fit() during training , and forecast() during regular use
                □ Model parameters from fit() are saved and loaded to forecast()
            § Output: post-processed 7 day time series forecast for grid point.
                □ Dimensions: lead time (9), ensemble member (1000)
    - Reassemble grid
    - For each grid point: restore spatial correlations
        ○ Shuffle - restore spatial correlations
            § Output: shuffled 7 day forecast for grid point
    - Hydrological model
        ○ Output: 7 day ensemble soil moisture forecast (API)
"""

import datetime
import random
import time
from concurrent.futures._base import CancelledError

from dateutil.relativedelta import relativedelta
from timezonefinder import TimezoneFinder

import numpy as np
import xarray as xr
import dask as ds
import os
import gc
os.putenv('HDF5_USE_FILE_LOCKING', 'FALSE')
from netCDF4 import Dataset
from . import data_transfer, iris_regridding, transform, source_cube, forecast_cube, parameter_cube, settings
from .util import list_grouper, distributor




# nsw bounding coords
nsw_top_lat = -27.9675  # lat upper bound
nsw_bot_lat = -37.6423  # lat lower bound
nsw_left_lon = 140.6947  # lon lower bound
nsw_right_lon = 153.7687  # lon upper bound


def check_for_bad_smips():
    """\
    Attempt to deal with an error in the SMIPS data by not using data with abnormally high values.
    """
    from time import perf_counter
    print("Checking for Bad Regridded SMIPS")
    t1 = perf_counter()
    fname = settings.SMIPS_REGRID_AGG()
    cube = Dataset(fname, mode='a', format='NETCDF4')
    prcp = cube.variables['blended_precipitation']
    numslices = cube.dimensions['time'].size
    for i in range(numslices):
        m = np.nanmax(prcp[i])
        if m > 900:
            print(i, np.nanmax(prcp[i]))
            prcp[i] = -9999.0

    cube.close()
    t2 = perf_counter()
    elapsed = t2-t1
    print("Elapsed: {} secs".format(elapsed))

def bootstrap_jobs():
    """\
    These are run once to get everything built and in place before daily jobs can happen
    """
    data_transfer.get_latest_accessg_files(start_date=None, end_date=None)
    iris_regridding.run_smips_regridding(update_only=False)


def daily_jobs():
    """\
    Get all new daily data - should be done on a regular automatic basis.
    1. Get latest ACCESS-G files from NCI
    2. Regrid latest SMIPS files
    3. Aggregate new ACCESS-G and SMIPS files into ACCESS-G.nc and SMIPS.nc respectively
    """
    print("\nGetting today's ACCESS-G accum_prcp files...", flush=True)
    accessg_dates = data_transfer.get_latest_accessg_files()
    print("\nRegridding today's SMIPS Blended_rainfall files...", flush=True)
    smips_dates = iris_regridding.run_smips_regridding()
    print("\nAggregating new SMIPS files into SMIPS aggregate cube...", flush=True)
    source_cube.aggregate_netcdf(smips=True, just_dates=smips_dates)
    print("\nAggregating new ACCESS-G files into ACCESS-G aggregate cube...", flush=True)
    source_cube.aggregate_netcdf(accessg=True, just_dates=accessg_dates)
    print('Daily jobs done')

compressed = {'dtype': 'float64', 'complevel': 1,'zlib': True}
def split_accum_access_g():
    """\
    Splits accum_access_g into horizontal slices by lat.
    This should speed up reading of access_g a lot when doing parameter generation
    on the cluster. This prevents each cluster node trying to open the same file at the same time
    """
    lats, lons = source_cube.get_real_aps3_lat_lon_values(restrict_size=False)
    access_g_file = settings.ACCESS_G_AGG
    forecast = xr.open_dataset(access_g_file, decode_times=False, lock=False, engine='netcdf4')
    for lat in lats:
        lat_str = str(lat).replace("-", "s").replace(".", "_")
        sliced_g_file = access_g_file.replace(".nc", "{}.nc".format(lat_str))
        forecast_lat = forecast.sel(lat=lat, method=None)  # Method None is Exact match only
        print("Loading Access-G slice {} into RAM...".format(lat), flush=True)
        forecast_lat = forecast_lat.load()  # force into RAM (might not work!)
        print("Saving {}...".format(sliced_g_file), flush=True)
        forecast_lat.to_netcdf(sliced_g_file, format="NETCDF4", engine="netcdf4",
                               encoding={'accum_prcp': compressed, 'lat': compressed, 'lon': compressed, 'time': compressed})
        print("Saved {}".format(sliced_g_file), flush=True)


def _mp_create_parameter_files(lat, lons, altsource):
    if altsource:
        source = settings.config.get('ALTERNATE_ACCESS_G_AGG', False) or settings.ACCESS_G_AGG
        access_g_file = source
    else:
        access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_REGRID_AGG()
    observed = None
    forecast = None
    lat_lon_pairs = []
    for lon in lons:
        lat_lon_pairs.append((lat, lon))
    try:
        print("MP Process opening SMIPS: {}".format(smips_file), flush=True)
        observed = xr.open_dataset(smips_file, decode_times=False, lock=False, engine='netcdf4')
        observed_xa = observed['blended_precipitation']
        lat_str = str(lat).replace("-", "s").replace(".", "_")
        maybe_sliced_g_file = access_g_file.replace(".nc", "{}.nc".format(lat_str))
        if os.path.exists(maybe_sliced_g_file):
            print("MP Process will use pre-sliced ACCESS-G: {}".format(maybe_sliced_g_file), flush=True)
            forecast = xr.open_dataset(maybe_sliced_g_file, decode_times=False, lock=False, engine='netcdf4')
            try:
                forecast_xa = forecast['accum_prcp'].sel(lat=lat)
            except ValueError:
                forecast_xa = forecast['accum_prcp']
        else:
            print("MP Process will use full ACCESS-G: {}".format(access_g_file), flush=True)
            forecast = xr.open_dataset(access_g_file, decode_times=False, lock=False, engine='netcdf4')
            forecast_xa = forecast['accum_prcp'].sel(lat=lat)

        print("Attempting to load access_g at lat={} cross section into RAM".format(lat), flush=True)
        t1 = time.perf_counter()
        forecast_xa = forecast_xa.load()
        t2 = time.perf_counter()
        t3 = t2-t1
        t3_mins = int(t3) // 60
        t3_secs = int(t3 - (t3_mins * 60))
        print("Done preloading into RAM. Took {}m{}s.".format(str(t3_mins), str(t3_secs)), flush=True)
        for (_lat, lon) in lat_lon_pairs:
            try:
                parameter_cube.generate_forecast_parameters(_lat, lon, observed_xa, forecast_xa, skip_existing=False)
            except ValueError:  # coordinates don't have data or don't have a recognized timezone
                continue
    except BaseException as e:
        import traceback
        print("MP Params Process got an exception:\n{}".format(repr(e)), flush=True)
        print(str(e), flush=True)
        traceback.print_tb(e.__traceback__)
        print("\n", flush=True)
        raise
    finally:
        if observed:
            observed.close()
        if forecast:
            forecast.close()


def create_parameter_files(restrict_nsw: bool = False, multi_process=False, use_mpi=False):
    """\
    Generate forecast parameters and save to netCDF files.
    """
    lats, lons = source_cube.get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)

    access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_REGRID_AGG()
    observed = None
    forecast = None
    my_lats = lats
    started = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    if use_mpi:
        comm = use_mpi['comm']
        rank = use_mpi['rank']
        world_size = use_mpi['world_size']
        print("rank: {}, world_size: {}".format(rank, world_size))
        random.shuffle(my_lats)
        chunked_lats = []
        if rank == 0:
            chunked_lats = list(distributor(my_lats, world_size))
        my_lats = comm.scatter(chunked_lats, root=0)
        print("rank: {} started at {} - got lats: [{}]".format(rank, str(started), ",".join([str(la) for la in my_lats])), flush=True)
    try:
        if multi_process:
            if settings.config.get("PARAMS_USE_DASK", False):
                print("Using DASK for multiprocessing", flush=True)
                num_lats = len(my_lats)
                forecast = xr.open_dataset(access_g_file, decode_times=False, lock=False, engine='netcdf4')
                forecast_xa = forecast['accum_prcp'].chunk({"lat": num_lats})
            else:
                import multiprocessing as mp
                num_procs = int(settings.config.get("MP_NUM_PROCESSES", "2"))
                #mp_timeout = int(settings.config.get("MP_PROCESS_TIMEOUT", "36000"))  # 10 Hours
                if num_procs > 1:
                    print("Using mp with Procs={} for multiprocessing".format(str(num_procs)), flush=True)
                    mp_pool = mp.Pool(processes=num_procs)
                    with mp_pool as p:
                        jobs = {}
                        for i, lat in enumerate(my_lats):
                            j = i % 2 > 0
                            job_args = (lat, lons, j)
                            job_starttime = time.perf_counter()
                            job_task = p.apply_async(_mp_create_parameter_files, args=job_args)
                            jobs[i] = (job_args, job_starttime, False, job_task)
                        results = [ job_task.get() for job_num, (job_args, job_starttime, retried, job_task) in jobs.items() ]
                        print("Multiprocessing tasks finished...\n{}".format(str(results)), flush=True)
                    # while len(jobs):
                    #     to_remove = []
                    #     for job_num, (job_args, job_starttime, retried, job_task) in jobs.items():
                    #         job_task.wait(1)
                    #         if not job_task.ready():
                    #             mp_now_time = time.perf_counter()
                    #             elapsed = mp_now_time - job_starttime
                    #             if elapsed > mp_timeout:
                    #                 # Trigger a proper timeout:
                    #                 try:
                    #                     results[job_num] = job_task.get(1)
                    #                 except TimeoutError:
                    #
                    #         else:
                    #             # Get the result
                    #             results[job_num] = job_task.get(1)
                    #             to_remove.append(job_num)
                    #     for j in to_remove:
                    #         try:
                    #             del jobs[j]
                    #         except LookupError:
                    #             continue
                else:
                    results = [_mp_create_parameter_files(lat, lons, (i % 2 > 0)) for i, lat in enumerate(my_lats)]
                    print("Serial tasks finished...\n{}".format(str(results)), flush=True)
        else:
            observed = xr.open_dataset(smips_file, decode_times=False, lock=False, engine='netcdf4')
            forecast = xr.open_dataset(access_g_file, decode_times=False, lock=False, engine='netcdf4')
            observed_xa = observed['blended_precipitation']
            forecast_xa = forecast['accum_prcp']
            latlon_pairs = []
            if restrict_nsw:
                for lat in my_lats:
                    if nsw_bot_lat <= lat <= nsw_top_lat:
                        for lon in lons:
                            if nsw_left_lon <= lon <= nsw_right_lon:
                                latlon_pairs.append((lat, lon))
            else:
                for lat in my_lats:
                    for lon in lons:
                        latlon_pairs.append((lat, lon))
            count = 0
            for (lat, lon) in latlon_pairs:
                # if lat == -13.76953125 and lon == 130.341796875:
                #     pass
                # else:
                #     continue
                # if lat == -31.69921875 and lon == 141.240234375:  #Yancowinna area
                #     pass
                # else:
                #     continue

                try:
                    print("Workflow about to generate parameters for: {} {}".format(lat, lon), flush=True)
                    parameter_cube.generate_forecast_parameters(lat, lon, observed_xa, forecast_xa, skip_existing=True)
                    count = count + 1
                except ValueError as e:
                    # coordinates don't have data or don't have a recognized timezone
                    print(e, flush=True)
                    continue
                #if count > 116:
                #    break
    finally:
        if observed:
            observed.close()
        if forecast:
            forecast.close()
    if use_mpi:
        comm = use_mpi['comm']
        rank = use_mpi['rank']
        blocker = True
        finished = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        print("Rank {} tasks are finished at {} - Signalling to Rank 0.".format(rank, finished))
        blocker = comm.gather(blocker, root=0)
        if rank == 0:
            assert len(blocker) > 0
            now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            print("Rank 0 knows other MPI ranks are done at {} - Continuing to aggregate_netcdf().".format(now), flush=True)
            # continue down to aggregate_netcdf()
        else:
            assert blocker is None
            return None

    parameter_cube.aggregate_netcdf()


def quarterly_jobs():
    print("Creating parameter files")
    create_parameter_files(multi_process=True)


def grid_forecast(date: datetime.date, lat: float, lon: float, param_cube_slice=None):
    """\
    Create a forecast for a single set of coordinates and save to netCDF.
    @param date: date to forecast
    @param lat: latitude value
    @param lon: longitude value
    """
    mu, cov, tp = parameter_cube.read_parameters(lat, lon, param_cube_slice=param_cube_slice)
    transform.transform_forecast(lat, lon, date, mu, cov, tp)


def _mp_create_forecast_files(date, lat, lons, altsource):
    _p = None
    _s1 = None
    _s2 = None
    lat_lon_pairs = []
    for lon in lons:
        lat_lon_pairs.append((lat, lon))
    _error = None
    try:
        resp = []
        print("MP Process opening Parameter Cube slice for lat={}".format(lat))
        _p = parameter_cube.ParameterCubeReader()  # keep an instance around to prevent parameter cube from closing
        _p.preload_lat_chunk(lat)
        print("MP Process opening ACCESS_G Aggregated Cube slice for date={} and lat={}".format(date, lat))
        _s1 = source_cube.SourceCubeReader("accessg")
        _s1.preload_lat_chunk(lat, sel_date=date)
        # print("MP Process opening SMIPS Aggregated Cube slice for lat={}".format(lat))
        # _s2 = source_cube.SourceCubeReader("smips")
        # _s2.preload_lat_chunk(lat)
        for (lat, lon) in lat_lon_pairs:
            try:
                resp.append(grid_forecast(date, lat, lon))
            except ValueError:
                continue
        return resp
    except BaseException as e:
        import traceback
        print("MP Params Process got an exception:\n{}".format(repr(e)), flush=True)
        print(str(e), flush=True)
        traceback.print_tb(e.__traceback__)
        print("\n", flush=True)
        _error = e
        raise
    finally:
        if _p:
            del _p
        if _s1:
            del _s1
        if _s2:
            del _s2
        #exit(0)
        o = gc.collect()



def create_forecast_files(date: datetime.date=None, multi_process=False, restrict_to_nsw=False):
    """\
    Read the ACCESS-G data for a given forecast date, transform it using the predictor transformation parameters saved
    to netcdf, and save the resulting forecast to netcdf.
    - for all lead times for a date
    - for all grid points
    @param date date to forecast
    """
    if date is None:
        #TODO: replace nowz with pinned time at start of run
        nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        backtwoz = (nowz - relativedelta(days=2)).replace(hour=12, minute=0, second=0, microsecond=0)
        backonez = (nowz - relativedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        avail_offset = datetime.timedelta(hours=settings.ACCESS_G_UPDATE_HOUR)
        if nowz < backonez+avail_offset:
            date = backtwoz.date()
        else:
            date = backonez.date()
    print("Creating forecast for SMIPS using ACCESS-G at {}.".format(str(date)))
    lats, lons = source_cube.get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)
    #return forecast_cube.aggregate_netcdf(date)

    if multi_process:
        import multiprocessing as mp
        num_procs = int(settings.config.get("MP_NUM_PROCESSES", "2"))
        #mp_timeout = int(settings.config.get("MP_PROCESS_TIMEOUT", "36000"))  # 10 Hours
        if num_procs > 1:
            print("Using mp with Procs={} for multiprocessing".format(str(num_procs)), flush=True)
            mp_pool = mp.Pool(processes=num_procs, maxtasksperchild=2)
            with mp_pool as p:
                jobs = {}
                for i, lat in enumerate(lats):
                    j = i % 2 > 0
                    job_args = (date, lat, lons, j)
                    job_starttime = time.perf_counter()
                    job_task = p.apply_async(_mp_create_forecast_files, args=job_args)
                    jobs[i] = (job_args, job_starttime, False, job_task)
                    # if i >= num_procs:
                    #     break
                results = [ job_task.get() for job_num, (job_args, job_starttime, retried, job_task) in jobs.items() ]
                print("Multiprocessing tasks finished...\n{}".format(str(results)), flush=True)
        else:
            results = [_mp_create_forecast_files(date, lat, lons, (i % 2 > 0)) for i, (lat,lons) in enumerate(lats)]
            print("Serial tasks finished...\n{}".format(str(results)), flush=True)
    else:
        _p = None
        lat_lon_pairs = []
        if restrict_to_nsw:
            for lat in lats:
                if nsw_bot_lat <= lat <= nsw_top_lat:
                    for lon in lons:
                        if nsw_left_lon <= lon <= nsw_right_lon:
                            lat_lon_pairs.append((lat, lon))
        else:
            for lat in lats:
                for lon in lons:
                    lat_lon_pairs.append((lat, lon))
        try:
            _p = parameter_cube.ParameterCubeReader()  # keep an instance around to prevent parameter cube from closing
            for (lat, lon) in lat_lon_pairs:
                try:
                    grid_forecast(date, lat, lon)
                except ValueError:
                    continue
        finally:
            if _p:
                del _p  # now delete it, to allow parameter cube to close
    print("Done all the forecasts, now aggregating them into a forecast cube.", flush=True)
    forecast_cube.aggregate_netcdf(date)
    return date


def _mp_shuffle(lat, lati, lons, d, sample, alt):
    _s2 = None
    try:
        #print("MP Process opening Parameter Cube slice for lat={}".format(lat))
        # _p = parameter_cube.ParameterCubeReader()  # keep an instance around to prevent parameter cube from closing
        # _p.preload_lat_chunk(lat)
        #print("MP Process opening ACCESS_G Aggregated Cube slice for date={} and lat={}".format(d, lat))
        # _s1 = source_cube.SourceCubeReader("accessg")
        # _s1.preload_lat_chunk(lat, sel_date=date)
        print("MP Process opening SMIPS Aggregated Cube slice for lat={}".format(lat))
        _s2 = source_cube.SourceCubeReader("smips")
        _s2.preload_lat_chunk(lat)
        results = {}
        for loni, lon in enumerate(lons):
            results[(lat,lon)] = transform.shuffle(lati, loni, sample, d)
        return results
    finally:
        if _s2:
            del _s2
        gc.collect()


def create_shuffled_forecasts(for_date=None, restrict_to_nsw: bool = False, multi_process: bool = False):
    """\
    Create shuffled forecasts for all coordinates and save to netCDF.
    """
    #return forecast_cube.aggregate_netcdf(for_date, settings.FORECAST_SHUFFLE_PATH())
    if for_date is None:
        #TODO: replace nowz with pinned time at start of run
        nowz = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        backtwoz = (nowz - relativedelta(days=2)).replace(hour=12, minute=0, second=0, microsecond=0)
        backonez = (nowz - relativedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        avail_offset = datetime.timedelta(hours=settings.ACCESS_G_UPDATE_HOUR)
        if nowz < backonez+avail_offset:
            for_date = backtwoz.date()
        else:
            for_date = backonez.date()
    date_index_sample = source_cube.sample_date_indices()  # need this to be created once and then always be the same
    lats, lons = source_cube.get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)
    lat_lon_pairs = []
    if restrict_to_nsw:
        for lat in range(len(lats)):
            if nsw_bot_lat <= lats[lat] <= nsw_top_lat:
                for lon in range(len(lons)):
                    if nsw_left_lon <= lons[lon] <= nsw_right_lon:
                        lat_lon_pairs.append((lat, lon))
    else:
        pass
        for lat in range(len(lats)):
            for lon in range(len(lons)):
                lat_lon_pairs.append((lat, lon))

    if multi_process:
        import multiprocessing as mp
        num_procs = int(settings.config.get("MP_NUM_PROCESSES", "2"))
        if num_procs > 1:
            print("Using mp with Procs={} for multiprocessing".format(str(num_procs)), flush=True)
            mp_pool = mp.Pool(processes=num_procs, maxtasksperchild=2)
            with mp_pool as p:
                jobs = {}
                for i, lat in enumerate(lats):
                    j = i % 2 > 0
                    lati = i
                    job_args = (lat, lati, lons, for_date, date_index_sample, j)
                    job_starttime = time.perf_counter()
                    job_task = p.apply_async(_mp_shuffle, args=job_args)
                    jobs[i] = (job_args, job_starttime, False, job_task)
                    # if i >= num_procs:
                    #     break
                results = [ job_task.get() for job_num, (job_args, job_starttime, retried, job_task) in jobs.items() ]
                print("Multiprocessing tasks finished...\n{}".format(str(results)), flush=True)
        else:
            results = [_mp_shuffle(lat, i, lons, for_date, date_index_sample, (i % 2 > 0)) for i, lat in enumerate(lats)]
            print("Serial tasks finished...\n{}".format(str(results)), flush=True)
    else:
        for (lat, lon) in lat_lon_pairs:
            transform.shuffle(lat, lon, date_index_sample, for_date)

    forecast_cube.aggregate_netcdf(for_date, settings.FORECAST_SHUFFLE_PATH())




if __name__ == '__main__':
    print("Doing Daily Jobs")
    daily_jobs()
    print("Checking for Bad SMIPS")
    check_for_bad_smips()
    settings.update_setting("MP_NUM_PROCESSES", "1")
    create_parameter_files(restrict_nsw=False, multi_process=True)
    print("Creating Forecast files")
    #create_forecast_files(settings.placeholder_date)
    did_date = create_forecast_files()
    print("Creating Shuffled Forecasts.")
    #create_shuffled_forecasts(settings.placeholder_date)
    create_shuffled_forecasts(did_date)
    #create_shuffled_forecasts()
    #forecast_cube.aggregate_netcdf(settings.placeholder_date, settings.FORECAST_SHUFFLE_PATH())
    print('Done')

