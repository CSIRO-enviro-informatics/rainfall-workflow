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

def transform(data):
    """
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


def extract_fit_data(lat, lon, start_date, end_date):

    # check for timezone first in case of not usable location
    # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
    tf = TimezoneFinder(in_memory=True)
    timezone = tf.timezone_at(lng=lon, lat=lat)

    if not timezone: # timezone wasn't found, probably because the location is over water and doesn't have one
        print("Timezone not found")
        return 'Location over water'

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
        return 'Location over water'
    print('Timezone found')

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
        print('Coordinate does not contain observation values')
        return('Location over water')

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

        print(day, base_i, forecast_i)

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
    tf = TimezoneFinder(in_memory=True)
    timezone = tf.timezone_at(lng=lon, lat=lat)

    if not timezone: # timezone wasn't found, probably because the location is over water and doesn't have one
        print("Timezone not found")
        return 'Location over water'

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
        return 'Location over water'
    print('Timezone found')

    access_g_file = settings.ACCESS_G_AGG

    forecast = xr.open_dataset(access_g_file, decode_times=False)
    sel_date = (cdate - date(1900,1,1)).days

    forecast = forecast.sel(lat=lat, lon=lon, method='nearest')
    forecast = forecast.sel(time=sel_date)
    forecast_values = forecast['accum_prcp'].values

    base_i = 24*lead_time + int(utc_offset)  # day start
    forecast_i = base_i + 24  # day end

    print(lead_time, base_i, forecast_i)
    fc = forecast_values[forecast_i] - forecast_values[base_i]

    return fc


def transformation(lat, lon):
    """
    # Extract geographical grid point timeseries of SMIPS and ACCESS-G data and transform it.
    SMIPS: just take the entire time series
    ACCESS-G: take the time series 1-9 days out in the lead time dimension.
    """
    access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_AGG

    observed = xr.open_dataset(smips_file, decode_times=False)
    forecast = xr.open_dataset(access_g_file, decode_times=False)

    print(observed['time'])
    print(forecast['time'])

    exit()

    for lat_i in range(observed['lat'].size):
        for lon_i in range(observed['lon'].size):

            lat = float(observed['lat'][lat_i].data)
            lon = float(observed['lon'][lon_i].data)

            observed_grid = observed['blended_precipitation'][:, lat_i, lon_i].data
            #check if there's no data. if so, try next point
            if np.unique(observed_grid).size == 1:
                continue
            trans_observed = transform(observed_grid)
            trans_forecasts = []

            # find the time zone of SMIPS grid point to line up SMIPS data with ACCESS-G data
            tf = TimezoneFinder(in_memory=True)
            timezone = tf.timezone_at(lng=lon, lat=lat)


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
            elif ('Eucla' or 'Perth') in timezone:
                utc_offset = 8
            elif 'Sydney' in timezone:
                utc_offset = 10
            else:
                print('Unknown timezone?')
            print('Timezone found')

            for day in range(1, 10):
                base_i = 24*(day-1) + int(utc_offset)  # day start
                forecast_i = 24*day + int(utc_offset)  # day end
                base_grid = forecast['accum_prcp'][:, base_i, lat_i, lon_i].data
                accum_grid = forecast['accum_prcp'][:, forecast_i, lat_i, lon_i].data
                base_grid[base_grid <= -9999.0] = np.nan
                accum_grid[accum_grid <= -9999.0] = np.nan

                # subtract the previous day's accumulated value
                forecast_grid = accum_grid - base_grid
                trans_forecast = transform(forecast_grid)
                trans_forecasts.append(trans_forecast)
