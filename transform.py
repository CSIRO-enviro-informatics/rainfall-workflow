"""!
Extraction and transformation of data.
"""


from datetime import date
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats
import xarray as xr
from timezonefinder import TimezoneFinder
import pytrans
import settings
import dateutil.parser
from dateutil.relativedelta import relativedelta
import bjpmodel
import forecast_cube
import source_cube
from shuffle import shuffle_random_ties


def shuffle(lat: int, lon: int, date_index_sample: list):
    """!
    Restore spacial correlations inside a forecast (inside each ensemble member) using the Schaake shuffle.
    Save results to individual netCDF grid files.
    @param lat: latitude index
    @param lon: longitude index
    @param date_index_sample: list of date indices
    """
    observed = xr.open_dataset(settings.SMIPS_AGG, decode_times=False)
    fc = xr.open_dataset(settings.forecast_agg(settings.placeholder_date))
    lats, lons = source_cube.get_lat_lon_values()

    obs_pre_shuffle = np.zeros((9, 1000))
    coord_observed = observed.blended_precipitation.values[:, lat, lon]
    fc_pre_shuffle = fc.forecast_value.values[lat, lon]

    for i in range(len(date_index_sample)):
        for lead in range(9):
            # read in the SMIPS data for the date and save to array
            obs_pre_shuffle[lead, i] = coord_observed[date_index_sample[i] + lead]

    # make and fill smips array
    for lead in range(9):
        fc_to_shuffle = fc_pre_shuffle[lead]
        obs_to_shuffle = obs_pre_shuffle[lead]
        # pass the SMIPS and forecast data arrays to shuffle function

        shuffled_fc = shuffle_random_ties(fc_to_shuffle, obs_to_shuffle)
        # save shuffled_fc to netcdf
        forecast_cube.add_to_netcdf_cube(settings.shuffled_forecast_filename(settings.placeholder_date, lats[lat], lons[lon]),
                                         lead, shuffled_fc)


def transform_forecast(lat, lon, d, mu, cov, tp):
    # read a sample grid point transform parameter set

    for lt in range(9):
        predictor_tp = tp[lt][0]
        predictand_tp = tp[lt][1]

        data = extract_data(lat, lon, d, lt)

        # create a pylogsinh object and then call forecast with it and the t_parameters
        bjp_model = bjpmodel.BjpModel(2, [10, 10], burn=1000, chainlength=2000, seed='random')

        # transformers
        ptort = pytrans.PyLogSinh(predictor_tp[0], predictor_tp[1], predictor_tp[2])
        ptandt = pytrans.PyLogSinh(predictand_tp[0], predictand_tp[1], predictand_tp[2])

        res = bjp_model.forecast([data], [ptort, ptandt], mu[lt].data, cov[lt].data)
        fc = res['forecast'][:, 1]
        # the returned trans data is your forecast
        forecast_cube.add_to_netcdf_cube(settings.forecast_filename(d, lat, lon), lt, fc)


def find_timezone(lat, lon):
    tf = TimezoneFinder(in_memory=True)
    timezone = tf.timezone_at(lng=lon, lat=lat)

    if not timezone:  # timezone wasn't found, probably because the location is over water and doesn't have one
        raise ValueError("Timezone does not exist")
        #return 'Location over water'

    if ('Brisbane' or 'Lindeman') in timezone:
        utc_offset = 10
    elif 'Melbourne' in timezone:
        utc_offset = 10
    elif 'Adelaide' in timezone:
        utc_offset = 9.5
    elif 'Darwin' in timezone:
        utc_offset = 9.5
    elif 'Hobart' in timezone:
        utc_offset = 10
    elif 'Eucla' in timezone or 'Perth' in timezone:
        utc_offset = 8
    elif 'Sydney' in timezone:
        utc_offset = 10
    else:
        print('Unknown timezone?', timezone)
        raise ValueError("Timezone exists but is not in Australia")
    print('Timezone found')
    return utc_offset


def extract_fit_data(lat, lon, start_date, end_date):

    # check for timezone first in case of not usable location
    # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
    utc_offset = find_timezone(lat, lon)

    access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_AGG

    observed = xr.open_dataset(smips_file, decode_times=False)

    # need to get some extra days of obs to account for lead time
    sel_date_deltas = []
    cdate = start_date
    while cdate <= (end_date + relativedelta(days=15)):
        sel_date_deltas.append((cdate - date(1900,1,1)).days)
        cdate += relativedelta(days=1)

    observed = observed.sel(lat=lat, lon=lon, method='nearest')
    observed = observed.sel(time=sel_date_deltas)
    observed_values = observed['blended_precipitation'].values

    if np.isnan(observed_values).all():
        #print('Coordinate does not contain observation values')
        raise ValueError("Coordinate does not contain observation values")

    forecast = xr.open_dataset(access_g_file, decode_times=False)

    sel_date_deltas = []
    cdate = start_date
    while cdate <= end_date:
        sel_date_deltas.append((cdate - date(1900,1,1)).days)
        cdate += relativedelta(days=1)
    num_forecasts = len(sel_date_deltas)

    forecast = forecast.sel(lat=lat, lon=lon, method='nearest')
    forecast = forecast.sel(time=sel_date_deltas)
    forecast_values = forecast['accum_prcp'].values

    fit_ptor_data = np.empty((num_forecasts, 9))
    fit_ptand_data = np.empty((num_forecasts, 9))

    for day in range(9):
        base_i = 24*day + int(utc_offset)  # day start
        forecast_i = base_i + 24  # day end

        #print(day, base_i, forecast_i)

        ### WARNING: TO DO: DOUBLE-CHECK ALIGNMENT OF ACCESS-G AND SMIPS
        fc = forecast_values[:, forecast_i] - forecast_values[:, base_i]
        obs = observed_values[(day+2):(day+2+num_forecasts)]

        #print(stats.spearmanr(fc, obs, nan_policy='omit')[0]) # breaks if data is nan

        fit_ptor_data[:, day] = fc
        fit_ptand_data[:, day] = obs

    res = {}
    res['ptor'] = fit_ptor_data
    res['ptand'] = fit_ptand_data

    return res


def extract_data(lat, lon, cdate, lead_time):
    # check for timezone first in case of not usable location
    # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
    utc_offset = find_timezone(lat, lon)

    access_g_file = settings.ACCESS_G_AGG

    forecast = xr.open_dataset(access_g_file, decode_times=False)
    sel_date = (cdate - date(1900,1,1)).days

    forecast = forecast.sel(lat=lat, lon=lon, method='nearest')
    forecast = forecast.sel(time=sel_date)
    forecast_values = forecast['accum_prcp'].values

    base_i = 24*lead_time + int(utc_offset)  # day start
    forecast_i = base_i + 24  # day end

    #print(lead_time, base_i, forecast_i)
    fc = forecast_values[forecast_i] - forecast_values[base_i]

    return fc


def transform(data):
    """
    Deprecated.
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
