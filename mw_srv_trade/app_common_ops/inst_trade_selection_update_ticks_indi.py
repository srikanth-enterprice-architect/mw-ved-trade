from datetime import datetime
import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *
from mw_srv_trade.app_common_trade_logger.logger import cus_logger


def ticks_indi_file_update():
    trade_inst = pd.read_csv('resources/account_data/trade_inst.csv')
    ticks_indi_template = pd.read_csv('resources/account_data/ticks_indi_template.csv')
    ticks_indi_file = pd.DataFrame()
    for trade_inst_index, trade_inst_record in trade_inst.iterrows():
        if trade_inst_record.inst_date_diff not in [0]:
            segment = trade_inst_record.inst_segment
            inst_name = trade_inst_record.inst_name
            inst_name_tem = ticks_indi_template.instrument_name
            ticks_indi_template_ = ticks_indi_template[inst_name_tem == segment + ':' + inst_name]
            ticks_indi_file = pd.concat([ticks_indi_file, ticks_indi_template_.tail(1)], ignore_index=True)
    ticks_indi_file.to_csv('resources/account_data/ticks_indi.csv', index=False)


def trade_ready_instruments_df():
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
        instruments[new_columns] = instruments['Symbol Details'].str.split(' ', expand=True).iloc[:, 0:5]
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


def ticks_ind_collect_instrument(instrument_trading_symbol):
    ticks_ind = pd.read_csv(TICKS_IND_FILE)
    ticks_ind = ticks_ind[ticks_ind.instrument_trading_symbol == instrument_trading_symbol]
    return ticks_ind
