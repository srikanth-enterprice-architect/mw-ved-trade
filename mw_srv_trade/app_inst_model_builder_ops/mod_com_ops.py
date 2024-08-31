import datetime as dt
from datetime import date, timedelta
import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import ACCOUNTS_FOLDER
from mw_srv_trade.app_common_trade_lib.session_builder.retrive_request_token import super_user_session
from mw_srv_trade.app_common_trade_logger.logger import cus_logger
from mw_srv_trade.app_inst_mkt_data_ops.inst_mkt_data_feed_fetch_ops import data_fetch_retailer


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


def get_day_high_low(sp_user_session, ticks_info_record):
    from_date = (date.today()) - timedelta(days=5)
    to_date = date.today()
    ticks = ticks_info_record.inst_segment + ':' + ticks_info_record.inst_data_name + '-INDEX'
    response = data_fetch_retailer(ticks, '1', from_date, to_date, sp_user_session)
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
