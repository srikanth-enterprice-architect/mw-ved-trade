import datetime

import pandas as pd
from mw_srv_trade.constants.file_constants import *
from datetime import datetime, date

from mw_srv_trade.trade_lib.session_builder.retrive_request_token import create_user_session
from mw_srv_trade.trade_logger.logger import cus_logger

cus_logger.setLevel(10)


def read_user_info():
    """
    All user info will read and send as data-frame
    """
    cus_logger.info("stated read user info data from user_info file")
    user_info_excel = pd.read_csv(USER_INPUTS_FILE)
    return pd.DataFrame(user_info_excel).astype(str)


def write_user_info(user_id, auth_code, refresh_token, access_token, auth_token_date):
    """
    generated user session tokens will be stored
    """
    cus_logger.info("storing user token information in the file sys")
    user_records = pd.read_csv(USER_INPUTS_FILE)
    user_records_data = pd.DataFrame(user_records).astype(str)
    for user_record_position, user_info_record in user_records_data.iterrows():
        if user_info_record['user_id'] == user_id:
            user_records_data.at[user_record_position, 'auth_code'] = auth_code
            user_records_data.at[user_record_position, 'day'] = date.today().day
            user_records_data.at[user_record_position, 'refresh_token'] = refresh_token
            user_records_data.at[user_record_position, 'access_token'] = access_token
            user_records_data.at[user_record_position, 'auth_token_date'] = auth_token_date
    user_records_data['login_pin'].astype(str)
    user_records_data.to_csv(USER_INPUTS_FILE, index=False)
    return user_records_data


def update_auto_inputs(env, minutes, super_trend_period, super_trend_multiplier):
    """
    user input data would be updated on auto_input csv file
   """
    cus_logger.info("updating the user input data into the auto_input.csv file")
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    auto_inputs = pd.DataFrame(auto_inputs).astype(str)
    auto_inputs.at[0, 'scheduler_minutes'] = 1 * minutes
    auto_inputs.at[0, 'data_interval'] = str(1 * minutes)
    auto_inputs.at[0, 'super_trend_period'] = super_trend_period
    auto_inputs.at[0, 'super_trend_multiplier'] = super_trend_multiplier
    auto_inputs.at[0, 'env'] = env
    auto_inputs.to_csv(AUTO_INPUTS_FILE, index=False)


def download_each_user_tokens():
    """
    This code will be used to obtain the accessToken from the source system, which will then be used to access the
    order API and other services.
    """
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info = user_info[user_info.day != date.today().day]
    for user_record_position, user_record in user_info.iterrows():
        cus_logger.info("user(%s) session token generation started", user_record.user_id)
        user_kite_session, user_record = create_user_session(user_record, FIREFOX_DRIVER_PATH)
        write_user_info(user_record.user_id, user_record.auth_code, user_record.refresh_token, user_record.access_token,
                        user_record.auth_token_date)
    cus_logger.info("session token generation completed ")


def ticks_indi():
    """
    instruments data will read and send as dataframe
   """
    cus_logger.info("instruments are reading from ticks_ind.csv file")
    ticks_ind_excel = pd.read_csv(TICKS_IND_FILE)
    return pd.DataFrame(ticks_ind_excel)


def calculate_expiry_date():
    trade_inst = pd.read_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv')
    for ticks_info_position, ticks_info_record in trade_inst.iterrows():
        new_columns = ['script', 'year', 'mon', 'day', 'strike_price']
        instruments = pd.read_csv(INSTRUMENTS_DATA_FILE, low_memory=False)
        instruments = instruments[(instruments['Underlying symbol'] == ticks_info_record.inst_name)]
        instruments = instruments[(instruments['Option type'] == 'CE')]
        instruments[new_columns] = instruments['Symbol Details'].str.split(' ',expand=True).iloc[:, 0:5]
        instrument_date = (instruments['year'] + instruments['mon'] + instruments['day'])
        instrument_days = (pd.to_datetime(instrument_date, format='%y%b%d')).dt.date
        expiry_day = (instrument_days[instrument_days >= datetime.now().date()]).head(1)
        expiry_day_record = instruments.loc[expiry_day.index.values[0]]
        expiry_day = ((pd.to_datetime((expiry_day_record['year'] + expiry_day_record['mon'] + expiry_day_record['day']),
                                      format='%y%b%d')).date())
        trade_inst.loc[ticks_info_position, 'inst_expiry_date'] = str(expiry_day)
        trade_inst.loc[ticks_info_position, 'inst_current_date'] = str(datetime.now().date())
        trade_inst.loc[ticks_info_position, 'inst_date_diff'] = str((expiry_day - datetime.now().date()).days)
    trade_inst.to_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv', index=False)


def update_instr_file():
    trade_inst = pd.read_csv(f'{ACCOUNTS_FOLDER}trade_inst.csv')
    ticks_indi_create = pd.DataFrame()
    for ticks_info_position, ticks_info_record in trade_inst.iterrows():
        # if ticks_info_record.inst_day_high != 0:
        instruments = pd.read_csv(INSTRUMENTS_DATA_FILE, low_memory=False)
        instruments = instruments[(instruments['Scrip code'] == ticks_info_record.inst_name)
                                  & ((instruments['Option type'] == 'CE') | (instruments['Option type'] == 'PE'))]
        instruments[['script', 'year', 'mon', 'day', 'strike_price']] = instruments['Symbol Details'].str.split(' ',
                                                                                                                expand=True).iloc[
                                                                        :, 0:5]
        instruments['expiry_date'] = (
            pd.to_datetime((instruments['year'] + instruments['mon'] + instruments['day']), format='%y%b%d')).dt.date
        instruments = instruments[instruments['expiry_date'] == pd.to_datetime(ticks_info_record.inst_expiry_date)]
        instruments_buy = instruments[(instruments['Option type'] == 'PE') & (
                    pd.to_numeric(instruments['strike_price']) > float(ticks_info_record.inst_day_high))].sort_values(
            by=['strike_price'], ascending=True).head(1)
        instruments_sell = instruments[(instruments['Option type'] == 'CE') & (
                    pd.to_numeric(instruments['strike_price']) < float(ticks_info_record.inst_day_low))].sort_values(
            by=['strike_price'], ascending=False).head(1)
        ticks_indi_create = update_instrument_file(ticks_indi_create, instruments_buy, ticks_info_record)
        ticks_indi_create = update_instrument_file(ticks_indi_create, instruments_sell, ticks_info_record)

    ticks_indi_create.to_csv('resources/account_data/account/ticks_indi.csv', index=False)


def update_instrument_file(ticks_indi_create, ticks_info_position, ticks_info_record):
    trade_inst = pd.read_csv('resources/account_data/account/ticks_indi_template.csv')
    trade_inst = trade_inst[
        trade_inst['instrument_name'] == ticks_info_record.inst_segment + ":" + ticks_info_record.inst_name]
    new_row_data_dict = trade_inst.iloc[0].to_dict()
    new_row_data_dict['symbol_ticker'] = ticks_info_position['Symbol ticker'].values[0]
    new_row_data_dict['instrument_expiry_date'] = ticks_info_record.inst_expiry_date
    new_row_data_dict['candle_high'] = 0.0
    new_row_data_dict['candle_low'] = 0.0
    ticks_indi_create = ticks_indi_create.append(new_row_data_dict, ignore_index=True)
    return ticks_indi_create


def update_ticks_info():
    ticks_info = pd.read_csv('resources/account_data/account/ticks_indi.csv')
    for ticks_info_position, ticks_info_record in ticks_info.iterrows():
        if ticks_info_record.update_required == 'Y':
            instruments = pd.read_csv(INSTRUMENTS_DATA_FILE, low_memory=False)
            instruments = instruments[(instruments['Scrip code'] == ticks_info_record.instrument_name.split(':')[1])
                                      & (instruments['Option type'] == 'XX') & (instruments['Minimum lot size'] != 0)]
            instruments[['script', 'year', 'mon', 'day', 'strike_price']] = instruments['Symbol Details'].str.split(' ',
                                                                                                                    expand=True)
            instrument_days = (pd.to_datetime((instruments['year'] + instruments['mon'] + instruments['day']),
                                              format='%y%b%d')).dt.date
            expiry_day = (instrument_days[instrument_days > datetime.now().date()]).head(1)
            expiry_day_record = instruments.loc[expiry_day.index.values[0]]
            expir_day = ((pd.to_datetime(
                (expiry_day_record['year'] + expiry_day_record['mon'] + expiry_day_record['day']),
                format='%y%b%d')).date())
            ticks_info.loc[ticks_info_position, 'instrument_token'] = expiry_day_record['Expiry date']
            ticks_info.loc[ticks_info_position, 'instrument_expiry_date'] = expir_day.strftime("%d-%m-%Y")
            ticks_info.loc[ticks_info_position, 'instrument_trading_symbol'] = expiry_day_record['Expiry date']
    ticks_info.to_csv(TICKS_IND_FILE, index=False)


def collect_user_id(kite_session):
    user_info = read_user_info()
    return (user_info[user_info.api_key == kite_session.client_id]).user_id.values[0]


def ticks_ind_collect_instrument(instrument_trading_symbol):
    ticks_ind = pd.read_csv(TICKS_IND_FILE)
    ticks_ind = ticks_ind[ticks_ind.instrument_trading_symbol == instrument_trading_symbol]
    return ticks_ind


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
    current_time = datetime.now().time().strftime('%H:%M:%S')
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
