from datetime import timedelta

import numpy as np
import pandas as pd

from mw_minisoft.messaging_channel.teligram_channel import *
from mw_minisoft.trade_lib.tech_indicator.super_trend_builder import super_trend

cus_logger.setLevel(10)


def get_option_direction(instrument_details, sp_user_session):
    """
    This code will download the historical data for all indicators
    """
    try:
        from_date = (date.today()) - timedelta(days=5)
        to_date = date.today()
        tick = instrument_details['inst_option_name']
        response = download_data(tick, '5', from_date, to_date, sp_user_session)
        df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = (pd.to_datetime(df['date'], unit='s')).dt.tz_localize('utc').dt.tz_convert('Asia/kolkata')
        direction = get_super_trend_results(df, 7, 1)
        cus_logger.info("Last Record From new data set %s", str(pd.to_datetime(df.iloc[-1].date).time()))
    except Exception as exceptionMessage:
        raise exceptionMessage
    return direction


def get_option_direction_df(inst_records):
    inst_records["date_on"] = inst_records["date"].dt.date
    inst_records_unique_dates = pd.DataFrame({'days': inst_records["date_on"].unique()})
    inst_records_orders = pd.DataFrame()
    direction = 'down'
    for instr_days_record_position, instr_days_record in inst_records.iloc[3:].iterrows():
        previous_record = inst_records.iloc[instr_days_record_position - 1]
        current_record = inst_records.iloc[instr_days_record_position]
        if previous_record.super_trend_direction_7_3 != current_record.super_trend_direction_7_3:
            if current_record.super_trend_direction_7_3 == 'up':
                inst_records_orders = inst_records_orders.append(current_record)
    inst_records_orders = inst_records_orders[inst_records_orders.date_on == inst_records_unique_dates.iloc[-1].days]
    if inst_records_orders.shape[0] > 0:
        inst_records_order_min_dir = inst_records_orders.close.min()
        if inst_records.iloc[-1].close > inst_records_order_min_dir:
            direction = 'up'
    return direction




def download_data(ticks, interval, from_date, today, kite_session):
    response = None
    data = {"symbol": ticks.replace('_', ':'), "resolution": interval, "date_format": "1", "range_from": from_date,
            "range_to": today, "cont_flag": "0"}
    response = kite_session.history(data)
    if 'ok' in response['s']:
        cus_logger.info('data is downloading status %s and count of the data %s', response['s'],
                        len(response['candles']))
    if response is None:
        cus_logger.exception(ticks + ' instrument Service not Available')
    if 'error' in response['s']:
        cus_logger.exception(response['message'])
    return response


def get_super_trend_results(raw_df_date, period, multiplier):
    df_bk_converted_data = raw_df_date
    req_columns = ['true_range', 'average_true_range_period_7', 'final_ub', 'final_lb', 'uptrend', 'super_trend_7_3',
                   'super_trend_direction_7_3']
    df_bk_converted_data[req_columns] = None
    default_time_frame = super_trend(df_bk_converted_data, period, multiplier)
    default_time_frame['date'] = pd.to_datetime(default_time_frame['date'])
    return default_time_frame
