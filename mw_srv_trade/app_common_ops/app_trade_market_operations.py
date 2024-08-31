import datetime

import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *


def entry_time_l(instrument_name):
    entry_time_ = '09:00:00'
    if ('NSE' in instrument_name) and ('NIFTY' in instrument_name):
        entry_time_ = '09:15:00'
    elif ('NSE' in instrument_name) and ('INR' in instrument_name):
        entry_time_ = '09:00:00'
    elif 'MCX' in instrument_name:
        entry_time_ = '09:00:00'
    elif 'BSE' in instrument_name:
        entry_time_ = '09:15:00'
    return entry_time_


def exit_time_l(instrument_name):
    exit_time_ = '15:30:00'
    if 'NSE' in instrument_name and 'NIFTY' in instrument_name or 'BSE' in instrument_name:
        exit_time_ = '15:29:00'
    elif 'NSE' in instrument_name and 'INR' in instrument_name:
        exit_time_ = '16:58:00'
    elif 'MCX' in instrument_name:
        exit_time_ = '23:58:00'
    return exit_time_


def market_status(ticks):
    status = False
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    auto_inputs = pd.DataFrame(auto_inputs).astype(str)
    exit_time_ = exit_time_l(ticks)
    entry_time_ = entry_time_l(ticks)
    current_time = datetime.datetime.now().time().strftime('%H:%M:%S')
    before_mkt = current_time < exit_time_
    after_mkt = current_time > entry_time_
    if auto_inputs.iloc[0].env == 'test':
        status = True
    elif 'NSE' in ticks and 'NIFTY' in ticks and before_mkt and after_mkt:
        status = True
    elif 'BSE' in ticks and before_mkt and after_mkt:
        status = True
    elif 'NSE' in ticks and 'INR' in ticks and before_mkt and after_mkt:
        status = True
    elif 'MCX' in ticks and before_mkt and after_mkt:
        status = True
    return status
