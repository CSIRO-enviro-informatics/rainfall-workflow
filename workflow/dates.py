"""\
Functions for dealing with dates.
"""

import datetime
import glob
import os
import re
import numpy as np

from . import settings


def date2str(date: datetime.date):
    """\ Converts a datetime.date object to str in the form YYYYMMDD"""
    str_year = str(date.year)
    str_month = pad_with_zeros([str(date.month)])[0]
    str_day = pad_with_zeros([str(date.day)])[0]
    return "".join((str_year, str_month, str_day))


def datetime_from_iso(_d):
    try:
        _d = datetime.datetime.strptime(_d, "%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        try:
            _d =datetime.datetime.strptime(_d, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            _d = datetime.datetime.strptime(_d, "%Y-%m-%d")
            _d.replace(hour=12, minute=0, second=0)
    return _d.replace(tzinfo=datetime.timezone.utc)


def get_dates(start_date=datetime.date(2019, 1, 1), end_date=None, stringify=True):
    """this will give you a list containing all of the dates (not including end_date)"""
    if end_date is None:
        end_date = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).date()
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime.datetime):
        end_date = end_date.date()
    date_diff = (end_date-start_date).days
    if date_diff < 1:
        # start date and end date are the same, just give back one result
        if stringify:
            d = [date2str(start_date)]
        else:
            d = [start_date]
    else:
        if stringify:
            d = [date2str(start_date + datetime.timedelta(days=x)) for x in range(date_diff)]
        else:
            d = [start_date + datetime.timedelta(days=x) for x in range(date_diff)]
    return d


def old_get_dates(start_date=datetime.date(2019, 1, 1), end_date = datetime.date.today()):
    """\ Returns an array of str dates from a start to end date (not inclusive of end)"""
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


def get_full_month(year: str, month: str, days: list):
    """\
    Returns an array of str dates in a month
    @param year: year str
    @param month: month str (padded with zeros)
    @param days: str array of days in the month (padded with zeros)
    @return str array of YYYYMMDD dates in a month
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
    """\
    Make string reprentations of ints double digit if they're single digit by adding a leading zero.
    @param str_array: array of strings representing int days or months
    @return padded str_array
    """
    return [x if len(x) == 2 else '0' + x for x in str_array]


file_date_re_12z = re.compile(r"^.*(\d{8})12\.nc$", flags=re.IGNORECASE)
file_date_re_00z = re.compile(r"^.*(\d{8})(?:00)?\.nc$", flags=re.IGNORECASE)
def check_latest_local_files(file_path: str, smips=False):  # Adapted to be usable for getting regridded smips files
    """\
    Find date from which to get new files. Method uses "created" date on file. Might cause issues
    if the filesystem changes created date to current date when copying or modifying files.
    Checks only in this year and last year. If we're so far away that neither of those exist,
    then we need to modify this code.
    @param file_path: network path
    @return date one day after the modification date of the newest file
    """
    current_year = datetime.date.today().year
    last_year = current_year - 1
    if smips:
        mask = settings.smips_dest_mask()
    else:
        mask = '*.nc'
    list_of_files = glob.glob(file_path + str(current_year) + '/' + mask)
    list_of_files.extend(glob.glob(file_path + str(last_year) + '/' + mask))
    if '12z' in file_path:
        regex = file_date_re_12z
        hours = 12
    else:
        regex = file_date_re_00z
        hours = 0
    filedates = {}
    for l in list_of_files:
        match = regex.match(l)
        if not match:
            continue
        date = match[1]
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:8])
        dt = datetime.datetime(year, month, day, hours, tzinfo=datetime.timezone.utc)
        filedates[dt] = l
    if len(filedates) < 1:
        return (None, None)
    latest_file_date = max(filedates.keys())
    latest_file = filedates[latest_file_date]
    return (latest_file_date, latest_file) #+ datetime.timedelta(days=1) add one?


def convert_date(date: np.datetime64):
    """\
    Converts np.datetime64 object (how date is stored in SMIPS) to datetime.date object.
    Note, dates in Python (and in reality) cannot have a concept of a timezone
    But for the purposes of this tool, consider these to be days based on 24h periods of UTC timezone.
    @param date date to convert
    @return datetime.date equivalent date
    """
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
    dt_date = datetime.datetime.utcfromtimestamp(timestamp)
    return dt_date.date()
