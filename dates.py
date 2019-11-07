import datetime
import glob
import os
import numpy as np


def date2str(date):
    """ Converts a datetime.date object to str in the form DDMMYYYY"""
    str_year = str(date.year)
    str_month = pad_with_zeros([str(date.month)])[0]
    str_day = pad_with_zeros([str(date.day)])[0]
    return str_year + str_month + str_day


def get_dates(start_date=datetime.date(2019, 1, 1), end_date = datetime.date.today()):
    """ Returns an array of str dates from a start to end date (not inclusive of end)"""
    if end_date < start_date:
        raise ValueError('End date cannot be before start date, start date:' + str(start_date) + ', end date:' + str(end_date))
    if end_date == start_date:
        raise ValueError('End date is same as start date')
    if start_date >= datetime.date.today():
        raise ValueError('Cannot have start date on or after today')
    years = [str(x) for x in range(start_date.year, end_date.year+1)]

    num_months = [x for x in range(1,13)]
    num_days = [x for x in range(1,32)]

    if start_date.year == end_date.year:
        months = [str(x) for x in num_months if start_date.month <= x <= end_date.month]
    else:
        months = [str(x) for x in num_months if start_date.month <= x]
        months.extend([str(x) for x in num_months if x <= end_date.month])
    months = pad_with_zeros(months)
    #print(months)

    # Not equal to today as won't have data that recent.
    days = [str(x) for x in num_days]
    days = pad_with_zeros(days)
    #print(days)

    dates = []
    for year in years:
        for month in months:
            month_dates = []
            if year == start_date.year and month < start_date.month:
                raise ValueError('Dates start before start date')
            if year == end_date.year and month > end_date.month:
                raise ValueError('Dates continue after end date')

            # if all the dates are in the span of the current month
            if (start_date.month == end_date.month) and start_date.year == end_date.year and int(month) == start_date.month:
                month_dates = [year + month + day for day in days[start_date.day - 1:end_date.day - 1]]
            # if the current month is the start month but not the end
            elif int(month) == start_date.month and int(year) == start_date.year:
                # depending on how many days are in the month
                if month == '02':
                    month_dates = [year + month + day for day in days[start_date.day - 1:28]]
                elif month in ['04', '06', '09', '11']:
                    month_dates = [year + month + day for day in days[start_date.day - 1:30]]
                else:
                    month_dates = [year + month + day for day in days[start_date.day - 1:31]]

            # if the current month is the end month
            elif (int(month) == end_date.month) and (int(year) == end_date.year):
                month_dates = [year + month + day for day in days[:end_date.day - 1]]

            # if any other condition
            else:
                month_dates = get_full_month(year, month, days)
            dates.extend(month_dates)
        print(dates)
    return dates


def get_full_month(year, month, days):
    """
    Returns an array of str dates in a month
    Parameters:
        year -- year string
        month -- month string (padded with zeros)
        days -- string array of days in the month (padded with zeros)
    Return: str array of YYYYMMDD dates in a month
    """
    if month == '02':
        month_dates = [year + month + day for day in days[:28]]
    elif month in ['04', '06', '09', '11']:
        month_dates = [year + month + day for day in days[:30]]
    else:
        month_dates = [year + month + day for day in days[:31]]
    #print(month_dates)
    return month_dates


def pad_with_zeros(str_array):
    """" Makes string reprentations of ints double digit if they're single digit by adding a leading zero.
    Parameters:
        str_array -- array of strings usually representing days or months
    Return: padded str_array
    """
    return [x if len(x) == 2 else '0' + x for x in str_array]


# Takes a network path, returns the date one day after the modification of the newest file
def get_start_date(file_path):  # Adapted to be usable for getting regridded smips files

    list_of_files = glob.glob(file_path + '2019/*.nc')
    latest_file = max(list_of_files, key=os.path.getctime)
    #print(latest_file)
    latest_file = latest_file.rsplit('_')[-1]
    if '12z' in file_path:
        start_date = latest_file.split('12.nc', 1)[0]
    else:
        start_date = latest_file.split('.nc', 1)[0]
    #print(start_date)

    start_year = int(start_date[:4])
    start_month = int(start_date[4:6])
    start_day = int(start_date[6:8])
    #print(start_year, start_month, start_day)
    # Return date of one day after newest file
    return datetime.date(start_year, start_month, start_day) + datetime.timedelta(days=1)


def convert_date(date):
    """ Converts np.datetime64 object to datetime.date object"""
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
    dt_date = datetime.datetime.utcfromtimestamp(timestamp)
    return dt_date.date()


#if __name__ == '__main__':
#    get_dates(start_date=datetime.date(2019, 2, 1))
