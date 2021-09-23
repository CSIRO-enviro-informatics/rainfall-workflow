"""\
Extraction and transformation of data.
"""
import time
from datetime import date
#import matplotlib.pyplot as plt
from functools import lru_cache
from operator import setitem

import numpy as np
import scipy.stats as stats
import xarray as xr
from timezonefinder import TimezoneFinder

import dateutil.parser
from dateutil.relativedelta import relativedelta

from . import settings, bjpmodel, forecast_cube, source_cube
from .shuffle import shuffle_random_ties

import pytrans

def shuffle(lati: int, loni: int, date_index_sample: list, at_date: date = None):
    """\
    Restore spacial correlations inside a forecast (inside each ensemble member) using the Schaake shuffle.
    Save results to individual netCDF grid files.
    @param lat: latitude index
    @param lon: longitude index
    @param date_index_sample: list of date indices
    @param at_date: Which date should we shuffle for?
    """
    lats, lons = source_cube.get_real_aps3_lat_lon_values(restrict_size=settings.restrict_size)
    real_lat = lats[lati]
    real_lon = lons[loni]
    print("\nDoing schaake suffle for {} {}".format(real_lat, real_lon), flush=True)

    if at_date is None:
        at_date = settings.FORECAST_DATE
    fc = xr.open_dataset(settings.forecast_agg(at_date))
    try:
        fc_pre_shuffle = fc.forecast_value.isel(lat=lati, lon=loni).values
    except LookupError:
        # That lat/lon pair doesn't exist in the forecast cube
        print("No forecast exists for {} {}, skipping...".format(real_lat, real_lon), flush=True)
        fc.close()
        return None
    if np.all(np.isnan(fc_pre_shuffle[0])):
        print("All NaN in forecast for {} {}, skipping...".format(real_lat, real_lon), flush=True)
        fc.close()
        return None
    # observed = xr.open_dataset(settings.SMIPS_REGRID_AGG(),  decode_times=False)
    # try:
    #     coord_observed = observed.blended_precipitation.sel(lat=real_lat, lon=real_lon).values
    # except LookupError:
    #     # that lat/lon pair doesn't exist in the SMIPS data
    #     print("No SMIPS observed exists for {} {}, skipping...".format(real_lat, real_lon), flush=True)
    #     fc.close()
    #     observed.close()
    #     return None
    smips_reader = source_cube.SourceCubeReader("smips")
    try:
        coord_observed = smips_reader.get_lat_lon(real_lat, real_lon).values
    except LookupError:
        # that lat/lon pair doesn't exist in the SMIPS data
        print("No SMIPS observed exists for {} {}, skipping...".format(real_lat, real_lon), flush=True)
        fc.close()
        del smips_reader
        return None

    obs_pre_shuffle = np.zeros((1000, 9))
    for i in range(len(date_index_sample)):
        for lead in range(9):
            # read in the SMIPS data for the date and save to array
            obs_pre_shuffle[i, lead] = coord_observed[date_index_sample[i] + lead]

    # make and fill smips array
    for lead in range(9):
        fc_to_shuffle = fc_pre_shuffle[:, lead]
        obs_to_shuffle = obs_pre_shuffle[:, lead]
        # pass the SMIPS and forecast data arrays to shuffle function

        shuffled_fc = shuffle_random_ties(fc_to_shuffle, obs_to_shuffle)
        # save shuffled_fc to netcdf
        forecast_cube.add_to_netcdf_cube(settings.shuffled_forecast_filename(at_date, real_lat, real_lon),
                                         lead, shuffled_fc)
    fc.close()
    del smips_reader
    #observed.close()


def transform_forecast(lat, lon, d, mu, cov, tp):
    # read a sample grid point transform parameter set
    fc_cube_name = settings.forecast_filename(d, lat, lon)
    try:
        data_lt = extract_data(lat, lon, d, cache=False)
    except LookupError:
        raise RuntimeError("Access-G Data for {} is not available yet. Cannot do forecast for {}.".format(d, d))
    print("Doing forecast for {}, {}".format(lat, lon), flush=True)

    all_ptors =  [ tp[lt][0] for lt in range(9) ]
    all_ptands = [ tp[lt][1] for lt in range(9) ]
    # with open("/tmp/tptand.csv", "w") as f:
    #     f.write("lt1, lt2, lt3, lt4, lt5, lt6, lt7, lt8, lt9\n")
    #     s = ", ".join("({})".format(",".join(str(k) for k in j)) for j in all_ptands)
    #     f.write(s)
    #     f.write("\n")
    # with open("/tmp/tptor.csv", "w") as f:
    #     f.write("lt1, lt2, lt3, lt4, lt5, lt6, lt7, lt8, lt9\n")
    #     s = ", ".join("({})".format(",".join(str(k) for k in j)) for j in all_ptors)
    #     f.write(s)
    #     f.write("\n")

    for lt in range(9):
        predictor_tp = tp[lt][0]
        predictand_tp = tp[lt][1]

        data = data_lt[lt]

        # create a pylogsinh object and then call forecast with it and the t_parameters
        bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=2000, seed='random')

        # transformers
        ptort = pytrans.PyLogSinh(predictor_tp[0], predictor_tp[1], predictor_tp[2])
        ptandt = pytrans.PyLogSinh(predictand_tp[0], predictand_tp[1], predictand_tp[2])
        print("\tTransform forecast for lead time {}".format(lt), flush=True)
        res = bjp_model.forecast([data], [ptort, ptandt], mu[lt].data, cov[lt].data)
        fc = res['forecast'][:, 1]
        # the returned trans data is your forecast
        forecast_cube.add_to_netcdf_cube(fc_cube_name, lt, fc)


@lru_cache()
def get_timezonefinder():
    tf = TimezoneFinder(in_memory=True)
    return tf

def find_timezone(lat, lon):
    tf = get_timezonefinder()
    timezone = tf.timezone_at(lng=lon, lat=lat)

    if not timezone:  # timezone wasn't found, probably because the location is over water and doesn't have one
        raise ValueError("Timezone does not exist")
        #return 'Location over water'
    if 'Sydney' in timezone:  # Assuming no Daylight savings time for NSW/ACT
        utc_offset = 10
    elif 'Brisbane' in timezone or 'Lindeman' in timezone:
        utc_offset = 10
    elif 'Melbourne' in timezone:  # Assuming no Daylight savings time for VIC
        utc_offset = 10
    elif 'Adelaide' in timezone:
        utc_offset = 9.5
    elif 'Darwin' in timezone:
        utc_offset = 9.5
    elif 'Hobart' in timezone or 'Currie' in timezone:
        utc_offset = 10
    elif 'Perth' in timezone:
        utc_offset = 8
    elif 'Eucla' in timezone:
        utc_offset = 8.75
    elif 'Broken_Hill' in timezone: #Yancowinna
        utc_offset = 9.5
    elif "Howe" in timezone:
        utc_offset = 10.5
    else:
        print('Unknown timezone?', timezone)
        raise ValueError("Timezone exists but is not in Australia")
    print('Timezone found {}'.format(timezone))
    return utc_offset


def extract_fit_data(lat, lon, observed_xa, forecast_xa, start_date, end_date):
    print("extracting fit data for {} {} for dates: {} to {}".format(lat, lon, start_date, end_date), flush=True)
    # check for timezone first in case of not usable location
    # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
    try:
        utc_offset = find_timezone(lat, lon)
    except ValueError:
        print("point not over Australia.. skipping.", flush=True)
        raise
    # need to get some extra days of obs to account for lead time

    d14 = relativedelta(days=14)
    d1 = relativedelta(days=1)
    date_epoch = date(1900, 1, 1)

    obs_date_deltas = []
    cdate = start_date
    while cdate <= (end_date + d14):
        obs_date_deltas.append((cdate - date_epoch).days)
        cdate += d1
    last_smips_offset = int(observed_xa.time[-1:].values[0])
    last_smips_date = date_epoch + relativedelta(days=last_smips_offset)
    for d in obs_date_deltas:
        if d > last_smips_offset:
            day = date_epoch + relativedelta(days=d)
            raise RuntimeError("Cannot extract fit data yet for day {}.".format(day))
    print("Loading SMIPS blended_precipitation values...", flush=True)
    t1 = time.perf_counter()
    observed_xa = observed_xa.sel(lat=lat, lon=lon, method='nearest')
    try:
        observed_xa = observed_xa.sel(time=obs_date_deltas)
    except KeyError:
        # This is where we know if we can do that date.
        print("Cannot extract fit data yet for {}.".format(end_date))
        raise

    # observed_values = observed_xr['blended_precipitation']#.values
    observed_values = observed_xa.values
    t2 = time.perf_counter()
    t3 = t2-t1
    print("Loading SMIPS slice took: {} seconds".format(t3))
    if lat == -28.066406 and lon == 140.71289:
        nans = np.isnan(observed_values)
        if np.any(nans):
            nans = np.argwhere(nans)
            raise RuntimeError("Bad SMIPS on day {}".format(str(nans)))
    if np.isnan(observed_values).all():
        print('Coordinate does not contain SMIPS observation values, skipping...', flush=True)
        raise ValueError("Coordinate does not contain observation values")

    forecaset_date_deltas = []
    cdate = start_date
    while cdate <= end_date:
        forecaset_date_deltas.append((cdate - date_epoch).days)
        cdate += d1
    num_forecasts = len(forecaset_date_deltas)
    
    print("Loading ACCESS-G forecast accum_prcp values...", flush=True)
    t1 = time.perf_counter()
    try:
        # Lat dim might not exist if we've restricted the DataArray an exact Lat before now
        if 'lat' in forecast_xa.dims:
            # method=None means exact
            forecast_xa = forecast_xa.sel(lat=lat, lon=lon, method=None)
        else:
            forecast_xa = forecast_xa.sel(lon=lon, method=None)
        # if we don't get an exact match, then throw an exception
    except Exception as e:
        print(e)
        raise
    try:
        forecast_xa = forecast_xa.sel(time=forecaset_date_deltas)
    except KeyError as e2:
        print("Not enough dates available in forecast cube!", flush=True)
        for d1 in reversed(forecaset_date_deltas):
            try:
                t1 = forecast_xa.sel(time=d1)
                del t1
            except KeyError:
                missing_date = date_epoch + relativedelta(days=d1)
                print("{} is missing!".format(missing_date), flush=True)
        raise
    #forecast_values = forecast_xr['accum_prcp']#.values

    #forecast_values = forecast_xa.values
    # nans = np.isnan(forecast_values)
    # if np.any(nans):
    #     nans = np.argwhere(nans)
    #     for n in nans:
    #         date_index = forecaset_date_deltas[n[0]]
    #         bad_date = date_epoch + relativedelta(days=int(date_index))
    #         raise RuntimeError("Bad ACCESS-G on date {}".format(bad_date))
    forecast_values = forecast_xa.values
    t2 = time.perf_counter()
    t3 = t2-t1
    print("Loading ACCESS-G slice took: {} seconds".format(t3))
    fit_ptor_data = np.empty((num_forecasts, 9))
    fit_ptand_data = np.empty((num_forecasts, 9))
    print("building fit data arrays", flush=True)
    t1 = time.perf_counter()
    NoneSlice = slice(None)
    #for day in range(9):
        # base_i = 24*day + int(utc_offset)  # day start
        # forecast_i = base_i + 24  # day end

        #print(day, base_i, forecast_i)

        ### WARNING: TO DO: DOUBLE-CHECK ALIGNMENT OF ACCESS-G AND SMIPS
        # fc = forecast_values[:, forecast_i] - forecast_values[:, base_i]
        # obs = observed_values[(day+2):(day+2+num_forecasts)]

        #print(stats.spearmanr(fc, obs, nan_policy='omit')[0]) # breaks if data is nan

        # fit_ptor_data[:, day] = fc
        # fit_ptand_data[:, day] = obs
    _ = [ setitem(fit_ptor_data, (NoneSlice, day), forecast_values[:, 24*day + int(utc_offset) + 24] - forecast_values[:, 24*day + int(utc_offset)]) for day in range(9) ]
    _ = [ setitem(fit_ptand_data, (NoneSlice, day), observed_values[(day+2):(day+2+num_forecasts)]) for day in range(9) ]

    # with open("/tmp/ptand.csv", "w") as f:
    #     f.write("lt1, lt2, lt3, lt4, lt5, lt6, lt7, lt8, lt9\n")
    #     for i in fit_ptand_data:
    #         s = ", ".join(str(j) for j in i)
    #         f.write(s)
    #         f.write("\n")
    # with open("/tmp/ptor.csv", "w") as f:
    #     f.write("lt1, lt2, lt3, lt4, lt5, lt6, lt7, lt8, lt9\n")
    #     for i in fit_ptor_data:
    #         s = ", ".join(str(j) for j in i)
    #         f.write(s)
    #         f.write("\n")
    t2 = time.perf_counter()
    t3 = t2-t1
    print("Building arrays took: {} seconds".format(t3))
    return {'ptor': fit_ptor_data, 'ptand': fit_ptand_data}



# def extract_data(lat, lon, cdate, cache=True):
#     # check for timezone first in case of not usable location
#     # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
#     utc_offset = find_timezone(lat, lon)
#
#     access_g_file = settings.ACCESS_G_AGG
#
#     forecast = xr.open_dataset(access_g_file, decode_times=False, cache=cache)
#     sel_date = (cdate - date(1900,1,1)).days
#
#     forecast = forecast.sel(lat=lat, lon=lon, method='nearest')
#     forecast = forecast.sel(time=sel_date)
#     forecast_values = forecast['accum_prcp'].values
#     fc = []
#     int_offset = int(utc_offset)
#     for i in range(9):
#         base_i = 24*i + int_offset  # day start
#         forecast_i = base_i + 24  # day end
#         #print(lead_time, base_i, forecast_i)
#         fc.append(forecast_values[forecast_i] - forecast_values[base_i])
#     forecast.close()
#     del forecast
#     return fc

def extract_data(lat, lon, cdate, cache=True):
    # check for timezone first in case of not usable location
    # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
    utc_offset = find_timezone(lat, lon)


    #sel_date = (cdate - date(1900,1,1)).days

    forecast_cube = source_cube.SourceCubeReader("accessg")
    forecast_pos = forecast_cube.get_lat_lon(lat, lon, sel_date=cdate)
    forecast_values = forecast_pos['accum_prcp'].values
    fc = []
    int_offset = int(utc_offset)
    for i in range(9):
        base_i = 24*i + int_offset  # day start
        forecast_i = base_i + 24  # day end
        #print(lead_time, base_i, forecast_i)
        fc.append(forecast_values[forecast_i] - forecast_values[base_i])
    return fc

def transform(data):
    """\
    Deprecated(?).
    Transform (normalise) a 1D time series representing a geographical grid point.
    Parameters:
        data -- 1D time series with valid float data and possible nans
    Return: transformed data
    """

    data = np.double(data)
    optim_data = data[np.logical_not(np.isnan(data))]
    lcens = 0.0
    scale = 5.0/np.max(optim_data)
    transform = pytrans.PyLogSinh(1.0, 1.0, scale)

    # Create a data subset without nans for optim_params
    optim = transform.optim_params(optim_data, lcens, True, True)
    trans_data = transform.transform_many(transform.rescale_many(data))

    #print(trans_data.min(), np.amin(trans_data), np.nanmin(trans_data), trans_data.max(), np.amax(trans_data), np.nanmax(trans_data))
    if np.isnan(trans_data.min()):
        print(trans_data)

    # Plot histograms of the original and transformed data
    # plt.hist(data, bins=50, density=True, alpha=0.5, label='orig', range=(np.nanmin(data), np.nanmax(data)))
    # plt.hist(trans_data, bins=50, density=True, alpha=0.5, label='trans_orig', range=(np.nanmin(trans_data), np.nanmax(trans_data)))
    # plt.show()
    return trans_data
