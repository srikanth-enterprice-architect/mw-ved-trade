import time
from datetime import date
from datetime import timezone
from datetime import timedelta
import datetime as dt
from datetime import timedelta
import pandas as pd
import os.path
from os.path import exists
from mw_minisoft.common_operatinos.common_ops import super_user_session
from mw_minisoft.constants.file_constants import *
from mw_minisoft.persistence_operations.account_management import ticks_indi, read_user_info, market_status
from mw_minisoft.trade_lib.strategy_builder.strategy_builder import strategy_data_builder_

from mw_minisoft.trade_logger.logger import cus_logger

cus_logger.setLevel(10)


def data_from_url(ticks, interval, from_date, today, kite_session):
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


def get_day_high_low(sp_user_session, ticks_info_record):
    from_date = (date.today()) - timedelta(days=5)
    to_date = date.today()
    ticks = ticks_info_record.inst_segment + ':' + ticks_info_record.inst_data_name + '-INDEX'
    response = data_from_url(ticks, '1', from_date, to_date, sp_user_session)
    df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = (pd.to_datetime(df['date'], unit='s')).dt.tz_localize('utc').dt.tz_convert('Asia/kolkata')
    cus_logger.info("Last Record From new data set %s", str(pd.to_datetime(df.iloc[-1].date).time()))
    instrument_history_data = df[df['date'] < dt.datetime.now().strftime('%Y-%m-%d %H:%M:00')]
    instrument_history_data['date_on_string'] = instrument_history_data["date"].dt.date.astype(str)
    instrument_history_data = instrument_history_data.sort_values(by='date')
    instrument_history_today = instrument_history_data[
        instrument_history_data.date_on_string >= str(instrument_history_data.iloc[-1].date.date())]
    instrument_history_today = instrument_history_today['close'].describe()
    return instrument_history_today


def download_data_url(ticks, interval, from_date, today, resource_path, kite_session, instrument_record):
    """
     download and modify the result set to fit the technical indicator
     """
    cus_logger.info("started downloading instrument data from dates today %s and from_day %s ", today,
                    from_date)
    instrument_history_data = pd.DataFrame()
    try:
        backup_file = DATA_BACK_UP_FOLDER + '/' + instrument_record.instrument_name.replace(':', '_')
        if not exists(backup_file):
            from_date = from_date - timedelta(days=30)
            response = data_from_url(ticks, interval, from_date, today, kite_session)
            df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = (pd.to_datetime(df['date'], unit='s')).dt.tz_localize('utc').dt.tz_convert('Asia/kolkata')
            cus_logger.info("Last Record From new data set %s", str(pd.to_datetime(df.iloc[-1].date).time()))
            instrument_history_data = df[df['date'] < dt.datetime.now().strftime('%Y-%m-%d %H:%M:00')]
            instrument_history_data.to_csv(backup_file, index=False)
            write_data_file(ticks, instrument_history_data, resource_path, interval)
        else:
            instrument_backup_data = pd.read_csv(backup_file)
            from_date = instrument_backup_data.iloc[-1].date.split(' ')[0]
            response = data_from_url(ticks, interval, from_date, today, kite_session)
            df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = (pd.to_datetime(df['date'], unit='s')).dt.tz_localize('utc').dt.tz_convert('Asia/kolkata')
            cus_logger.info("Last Record From new data set %s", str(pd.to_datetime(df.iloc[-1].date).time()))
            instrument_history_data = df[df['date'] < dt.datetime.now().strftime('%Y-%m-%d %H:%M:00')]
            instrument_history_data = instrument_history_data[
                instrument_history_data.date > (instrument_backup_data.iloc[-1].date)]
            instrument_backup_data = instrument_backup_data.append(instrument_history_data)
            instrument_backup_data.to_csv(backup_file, index=False)
            write_data_file(ticks, instrument_backup_data.tail(600), resource_path, interval)
    except Exception as exceptionMessage:
        cus_logger.exception(exceptionMessage)
    return instrument_history_data


def reset_values_day_high_low():
    trade_inst = pd.read_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv')
    for ticks_info_position, ticks_info_record in trade_inst.iterrows():
        trade_inst.loc[ticks_info_position, 'inst_day_high'] = 0
        trade_inst.loc[ticks_info_position, 'inst_day_low'] = 0
        trade_inst.loc[ticks_info_position, 'lock_low'] = 'N'
        trade_inst.loc[ticks_info_position, 'lock_high'] = 'N'
    trade_inst.to_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv', index=False)


def update_day_high_low():
    trade_inst = pd.read_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv')
    sp_user_session, sp_user_record = super_user_session()
    for ticks_info_position, ticks_info_record in trade_inst.iterrows():
        if ticks_info_record.inst_date_diff < 1:
            values_1 = get_day_high_low(sp_user_session, ticks_info_record)
            if ticks_info_record.lock_high == 'N':
                trade_inst.loc[ticks_info_position, 'inst_day_high'] = values_1['max']
            if ticks_info_record.lock_low == 'N':
                trade_inst.loc[ticks_info_position, 'inst_day_low'] = values_1['min']
        else:
            trade_inst.loc[ticks_info_position, 'inst_day_high'] = 0
            trade_inst.loc[ticks_info_position, 'inst_day_low'] = 0
    trade_inst.to_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv', index=False)


def update_trade_inst_based_position(inst_record, lock_type):
    inst_name = inst_record.instrument_name
    trade_inst = pd.read_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv')
    inst_name_filtered = trade_inst[trade_inst.inst_name == inst_name.split(":")[1]]
    if inst_record.symbol_ticker[-2:] == 'PE':
        if lock_type == 'up_entry':
            trade_inst.loc[inst_name_filtered.index[0], 'lock_high'] = 'Y'
        elif lock_type == 'up_exit':
            trade_inst.loc[inst_name_filtered.index[0], 'lock_high'] = 'N'
    elif inst_record.symbol_ticker[-2:] == 'CE':
        if lock_type == 'up_entry':
            trade_inst.loc[inst_name_filtered.index[0], 'lock_low'] = 'Y'
        elif lock_type == 'up_exit':
            trade_inst.loc[inst_name_filtered.index[0], 'lock_low'] = 'N'
    trade_inst.to_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv', index=False)


def generate_historical_data(auto_inputs):
    """
    This code will download the historical data for all indicators
    """
    try:
        ticks_indicator_df = ticks_indi()
        columns = ['instrument_trading_symbol', 'instrument_name']
        ticks_indicator_df = ticks_indicator_df[columns].drop_duplicates(subset=['instrument_name'])
        sp_user_session, sp_user_record = super_user_session()
        for record_position, instrument_record in ticks_indicator_df.iterrows():
            ticks = instrument_record.instrument_trading_symbol.replace(':', '_')
            data_interval = auto_inputs['data_interval'][0]
            if market_status(ticks):
                from_date = (date.today()) - timedelta(days=5)
                to_date = date.today()
                download_data_url(ticks, data_interval, from_date, to_date, TICKS_FOLDER_, sp_user_session,
                                  instrument_record)
            else:
                cus_logger.info('program is running in PROD -  %s Service not Available', ticks)

    except Exception as exceptionMessage:
        raise exceptionMessage


def write_data_file(instrument_token, instrument_data, resources_path, interval):
    instrument_data.to_csv(resources_path + str(instrument_token) + '_' + str(interval) + '.csv', index=False)


def read_data_file(instrument_token, resources_path, interval):
    file_name = resources_path + str(instrument_token) + '_' + str(interval) + '.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    else:
        cus_logger.error('File reading Failed -  %s', file_name)


def model_indicator_data_generator(auto_inputs):
    """
    will generate the technical values for inputted instrument data
    """
    ticks_indicator_df = ticks_indi()
    columns = ['instrument_trading_symbol', 'instrument_name']
    ticks_indicator_df = ticks_indicator_df[columns].drop_duplicates(subset=['instrument_name'])
    for record_position, indicator_record in ticks_indicator_df.iterrows():
        mk_status = market_status(indicator_record.instrument_name)
        if mk_status:
            instrument_history_data = read_data_file(indicator_record.instrument_trading_symbol.replace(':', '_'),
                                                     TICKS_FOLDER_, auto_inputs['data_interval'][0])

            instrument_history_data = strategy_data_builder_(instrument_history_data.copy(), auto_inputs,
                                                             indicator_record.instrument_trading_symbol)

            write_data_file(str(indicator_record.instrument_trading_symbol.replace(':', '_')),
                            instrument_history_data, TICKS_FOLDER_, str(auto_inputs['data_interval'][0]))
