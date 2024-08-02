from datetime import date
import datetime as dt
from datetime import timedelta
import pandas as pd
from os.path import exists
from mw_srv_trade.comm_ops.common_ops import super_user_session
from mw_srv_trade.constants.file_constants import *
from mw_srv_trade.mk_data_ops.data_read_write_ops import write_data_file
from mw_srv_trade.persist_ops.account_management import ticks_indi, market_status

from mw_srv_trade.trade_logger.logger import cus_logger

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
                instrument_history_data.date > instrument_backup_data.iloc[-1].date]
            # instrument_backup_data = instrument_backup_data.append(instrument_history_data)
            instrument_backup_data = pd.concat([instrument_backup_data, instrument_history_data], ignore_index=True)
            instrument_backup_data.to_csv(backup_file, index=False)
            write_data_file(ticks, instrument_backup_data.tail(600), resource_path, interval)
    except Exception as exceptionMessage:
        cus_logger.exception(exceptionMessage)
    return instrument_history_data


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


