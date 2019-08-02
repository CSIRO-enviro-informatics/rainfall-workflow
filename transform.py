import matplotlib.pyplot as plt
import numpy as np
from netCDF4 import Dataset
from timezonefinder import TimezoneFinder
import pytrans
import settings


def transform(data):
    """ Transform (normalise) a 1D time series representing a geographical grid point. """
    data = np.double(data) # because it was complaining
    optim_data = data[np.logical_not(np.isnan(data))]
    lcens = 0.0
    scale = 5.0/np.max(optim_data)
    transform = pytrans.PyLogSinh(1.0, 1.0, scale)

    # create a data subset without nans for optim_params
    optim = transform.optim_params(optim_data, lcens, True, False)
    print(np.array(optim))
    trans_data = transform.transform_many(transform.rescale_many(data))
    #print(trans_data.min(), np.amin(trans_data), np.nanmin(trans_data), trans_data.max(), np.amax(trans_data), np.nanmax(trans_data))
    if np.isnan(trans_data.min()):
        print(trans_data)
    plt.hist(data, bins=50, normed=True, alpha=0.5, label='orig', range=(np.nanmin(data), np.nanmax(data)))
    plt.hist(trans_data, bins=50, normed=True, alpha=0.5, label='trans_orig', range=(np.nanmin(trans_data), np.nanmax(trans_data)))
    plt.show()
    return trans_data


def transformation():
    """
    # Extract geographical grid point timeseries of SMIPS and ACCESS-G data and transform it.
    SMIPS: just take the entire time series
    ACCESS-G: take the time series 1-9 days out in the lead time dimension.
    """
    access_g_file = settings.ACCESS_G_AGG
    smips_file = settings.SMIPS_AGG

    observed = Dataset(smips_file)
    forecast = Dataset(access_g_file)


    for lat_i in range(observed['lat'].size):
        for lon_i in range(observed['lon'].size):
            lat = observed['lat'][lat_i].data
            lon = observed['lon'][lon_i].data

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
