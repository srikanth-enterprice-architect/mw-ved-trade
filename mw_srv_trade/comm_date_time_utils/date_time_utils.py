from datetime import date, timedelta

import pandas as pd


def data_from_and_today():
    from_date = (date.today()) - timedelta(days=5)
    to_date = date.today()
    return from_date, to_date


def ist_timezone(input_series):
    time_zone = 'Asia/kolkata'
    trans_data = (pd.to_datetime(input_series, unit='s')).dt.tz_localize('utc').dt.tz_convert(time_zone)
    return trans_data


def strf_time_convert(date_time):
    return date_time.strftime('%Y-%m-%d %H:%M:00')
