import matplotlib.pyplot as plt
import numpy as np
from netCDF4 import Dataset
from timezonefinder import TimezoneFinder
import pytrans
import settings

def transform(data):
    data = np.double(data) # because it was complaining
    lcens = 0.0
    scale = 5.0/np.max(data)

    transform = pytrans.PyLogSinh(1.0, 1.0, scale)

    transform.optim_params(data, lcens, True, False)
    trans_data = transform.transform_many(transform.rescale_many(data))
    print(trans_data)
    print(trans_data.min(), np.amin(trans_data), np.nanmin(trans_data), trans_data.max(), np.amax(trans_data), np.nanmax(trans_data))
    plt.hist(data, bins=50, normed=True, alpha=0.5, label='orig')
    plt.hist(trans_data, bins=50, normed=True, alpha=0.5, label='trans_orig', range=(np.nanmin(trans_data), np.nanmax(trans_data)))
    plt.show()
    return trans_data


def transformation():
    # Load data - access/smips
    # Extract 1 grid point of data # data must be in 1D array

    access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_AGG

    observed = Dataset(smips_file)
    forecast = Dataset(access_g_file)

    #print(forecast.accum_prcp.values)
    #print(observation.blended_precipitation.values)

    # Transform (normalise) a grid point of both observation and forecast data (separately)
    # SMIPS: just take the entire time series
    #observed_data = observed.blended_precipitation.values

    # loop through geo coordinates
    lat_i = 83
    lon_i = 123
    lat = observed['lat'][lat_i].data
    lon = observed['lon'][lon_i].data

    observed_grid = observed['blended_precipitation'][:, lat_i, lon_i].data # index corresponds to coords lon = 152.9297, lat = -28.35938 - random point consistently containing data # time, lat, lon
    #check if all values are nan. if so, try next point
    trans_observed = transform(observed_grid)

    # ACCESS-G: take the time series 1-9 days out in the lead time dimension.
    # Have to line up the time with SMIPS (and find SMIPS timezone to do so)
    # and since it's accumulated precipitation have to subtract the previous day's accumulated value
    # 1. 9am AEST = 23 UTC the previous day (so you'll want the previous day's access-g file to match smips) (this is plus 10 hours, or the 9th index)
    # 2. Initialise your base values to 0 and day 1 values = lead_time[24 + 10] - lead_time[10]
    # 2. Initialise your base values to 0 and day x=[1:9] values = lead_time[24x + 10] - lead_time[24(x-1) + 10]
    trans_forecasts = []
    # find the time zone of your grid point
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
    #elif 'Canberra' in timezone.id:
    #    utc_offset = 10
    else:
        print('Unknown timezone?')
    print('Timezone found')
    #forecast_data = forecast.accum_prcp.values # time, lead_time, lat, lon
    for day in range(1, 10):
        base_i = 24*(day-1) + int(utc_offset)
        forecast_i = 24*day + int(utc_offset)
        base_grid = forecast['accum_prcp'][:, base_i, lat_i, lon_i].data
        accum_grid = forecast['accum_prcp'][:, forecast_i, lat_i, lon_i].data
        forecast_grid = accum_grid - base_grid
        trans_forecast = transform(forecast_grid)
        trans_forecasts.append(trans_forecast)