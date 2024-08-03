import datetime as dt
from datetime import timedelta
import pandas as pd
from os.path import exists

from mw_srv_trade.comm_date_time_utils.date_time_utils import data_from_and_today, ist_timezone, strf_time_convert
from mw_srv_trade.comm_ops.common_ops import super_user_session
from mw_srv_trade.constants.file_constants import *
from mw_srv_trade.mk_data_ops.data_read_write_ops import write_data_file
from mw_srv_trade.persist_ops.account_management import ticks_indi, market_status
from mw_srv_trade.trade_logger.logger import cus_logger

cus_logger.setLevel(10)


def data_fetch_retailer(inst_name, interval, from_date, today, sp_user_session):
    retailer_resp = None
    retailer_resp_df_col = ['date', 'open', 'high', 'low', 'close', 'volume']
    retailer_data_req = {"symbol": inst_name.replace('_', ':'), "resolution": interval, "date_format": "1",
                         "range_from": from_date, "range_to": today, "cont_flag": "0"}
    retailer_resp = sp_user_session.history(retailer_data_req)
    retailer_res_df = pd.DataFrame(retailer_resp['candles'], columns=retailer_resp_df_col)
    retailer_res_df['date'] = ist_timezone(retailer_res_df['date'])
    if 'ok' in retailer_resp['s']:
        cus_logger.info('data is downloading status %s and count of the data %s', retailer_resp['s'],
                        len(retailer_resp['candles']))
    if retailer_resp is None:
        cus_logger.exception(inst_name + ' instrument Service not Available')
    if 'error' in retailer_resp['s']:
        cus_logger.exception(retailer_resp['message'])
    return retailer_res_df


def download_data_url(ticks, interval, from_date, today, resource_path, sp_user_session, instrument_record):
    """
     download and modify the result set to fit the technical indicator
     """
    cus_logger.info("started downloading inst data from dates today %s and from_day %s ", today, from_date)
    inst_his_data = pd.DataFrame()
    try:
        backup_file = DATA_BACK_UP_FOLDER + '/' + instrument_record.instrument_name.replace(':', '_')
        if exists(backup_file):
            if exists(resource_path):
                inst_backup_data = pd.read_csv(backup_file)
                from_date = inst_backup_data.iloc[-1].date.split(' ')[0]
                retailer_res_df = data_fetch_retailer(ticks, interval, from_date, today, sp_user_session)
                cus_logger.info("Last Record %s", str(pd.to_datetime(retailer_res_df.iloc[-1].date).time()))
                inst_his_data = retailer_res_df[retailer_res_df['date'] < strf_time_convert(dt.datetime.now())]
                inst_his_data = inst_his_data[inst_his_data.date > inst_backup_data.iloc[-1].date]
                inst_backup_data = pd.concat([inst_backup_data, inst_his_data], ignore_index=True)
                inst_backup_data.to_csv(backup_file, index=False)
                write_data_file(ticks, inst_backup_data.tail(600), resource_path, interval)
            else:
                inst_backup_data = pd.read_csv(backup_file)
                from_date = inst_backup_data.iloc[-1].date.split(' ')[0]
                retailer_res_df = data_fetch_retailer(ticks, interval, from_date, today, sp_user_session)
                cus_logger.info("Last Record %s", str(pd.to_datetime(retailer_res_df.iloc[-1].date).time()))
                inst_his_data = retailer_res_df[retailer_res_df['date'] < strf_time_convert(dt.datetime.now())]
                inst_his_data = inst_his_data[inst_his_data.date > inst_backup_data.iloc[-1].date]
                inst_backup_data = pd.concat([inst_backup_data, inst_his_data], ignore_index=True)
                inst_backup_data.to_csv(backup_file, index=False)
                write_data_file(ticks, inst_backup_data.tail(600), resource_path, interval)

        else:
            from_date = from_date - timedelta(days=30)
            retailer_res_df = data_fetch_retailer(ticks, interval, from_date, today, sp_user_session)
            cus_logger.info("Last Record %s", str(pd.to_datetime(retailer_res_df.iloc[-1].date).time()))
            inst_his_data = retailer_res_df[retailer_res_df['date'] < strf_time_convert(dt.datetime.now())]
            inst_his_data.to_csv(backup_file, index=False)
            write_data_file(ticks, inst_his_data, resource_path, interval)

    except Exception as exceptionMessage:
        cus_logger.exception(exceptionMessage)
    return inst_his_data


def generate_historical_data(auto_inputs):
    """
    This code will download the historical data for all indicators
    """
    try:
        ticks_indicator_df = ticks_indi()
        columns = ['instrument_trading_symbol', 'instrument_name']
        ticks_indicator_df = ticks_indicator_df[columns].drop_duplicates(subset=['instrument_name'])
        sp_user_session, sp_user_record = super_user_session()
        for record_position, inst_record in ticks_indicator_df.iterrows():
            ticks = inst_record.instrument_trading_symbol.replace(':', '_')
            data_interval = auto_inputs['data_interval'][0]
            if market_status(ticks):
                from_date, to_date = data_from_and_today()
                download_data_url(ticks, data_interval, from_date, to_date, TICKS_FOLDER_, sp_user_session,
                                  inst_record)
            else:
                cus_logger.info('program is running in PROD -  %s Service not Available', ticks)

    except Exception as exceptionMessage:
        raise exceptionMessage
