from datetime import date

import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *
from mw_srv_trade.app_common_trade_logger.logger import cus_logger

cus_logger.setLevel(10)


def read_instrument_tokens(instrument, strike_price, signal, exchange):
    """
        The 'ce' and 'pe' instruments  from the data.csv file were picked up by this piece of code.
    """
    inst_split_columns = ['script', 'year', 'mon', 'day', 'strike_price', 'option_type_1']
    try:
        cus_logger.info("read_instrument_tokens execution    ---    started")
        inst_filtered = filtered_instruments(inst_split_columns, instrument)
        inst_file = pd.read_csv(TICKS_IND_FILE)
        inst_file_filtered = inst_file[inst_file.instrument_name == exchange + ':' + instrument]

        if (signal == 'BUY') or ('up' in signal):
            inst_option_type_filter = inst_filtered[inst_filtered['option_type_1'] == 'CE']
            # inst_option_type_filter['strike_price'] = inst_option_type_filter['strike_price'].astype(float)
            inst_option_type_filter = inst_option_type_filter.astype({'strike_price': float})
            inst_option_type_filter = inst_option_type_filter.sort_values(by=['strike_price'], ascending=True)
            strike_price_filter = strike_price['low_price'] + inst_file_filtered.strike_price_position.values[0]
            price_filter_greater = inst_option_type_filter['strike_price'] >= strike_price_filter
            inst_strike_price_filter = inst_option_type_filter[price_filter_greater]
            inst_df_head = inst_strike_price_filter

        elif (signal == 'SELL') or ('down' in signal):
            inst_option_type_filter = inst_filtered[inst_filtered['option_type_1'] == 'PE']
            # inst_option_type_filter['strike_price'] = inst_option_type_filter['strike_price'].astype(float)
            inst_option_type_filter = inst_option_type_filter.astype({'strike_price': float})
            inst_option_type_filter = inst_option_type_filter.sort_values(by=['strike_price'], ascending=False)
            strike_price_filter = (strike_price['high_price'] + inst_file_filtered.strike_price_position.values[0])
            price_filter_less = inst_option_type_filter['strike_price'] <= strike_price_filter
            inst_strike_price_filter = inst_option_type_filter[price_filter_less]
            inst_df_head = inst_strike_price_filter

    except Exception as exceptionMessage:
        cus_logger.exception("read_instrument_tokens execution - failed error message %s", exceptionMessage)
    return inst_df_head.head(1)


def filtered_instruments(inst_split_columns, instrument):
    instruments = pd.read_csv(INSTRUMENTS_DATA_FILE, low_memory=False)
    instruments = instruments[instruments['Underlying symbol'] == instrument]
    instruments = instruments[instruments['Option type'].isin(['CE', 'PE'])]
    instruments[inst_split_columns] = instruments['Symbol Details'].str.split(' ', expand=True)
    inst_dates = pd.to_datetime((instruments['year'] + instruments['mon'] + instruments['day']), format='%y%b%d')
    instrument_days = inst_dates.dt.date
    expiry_day = (instrument_days[instrument_days > date.today()]).head(1)
    date_filter = (pd.to_datetime(expiry_day.values[0])).strftime('%y-%b-%d').split('-')
    inst_expiry_year_filter = instruments[instruments['year'] == date_filter[0]]
    inst_expiry_month_filter = inst_expiry_year_filter[inst_expiry_year_filter['mon'] == date_filter[1]]
    return inst_expiry_month_filter[inst_expiry_month_filter['day'] == date_filter[2]]


def download_write_instrument_tokens():
    # initialize data of lists.
    cus_logger.info('Downloading instrument tokens Started')
    inst_urls = pd.read_csv(ACCOUNTS_FOLDER + 'inst_links.csv')
    columns_names = ["Fytoken", "Symbol Details", "Exchange Instrument type", "Minimum lot size",
                     "Tick size", "ISIN", "Trading Session", "Last update date", "Expiry date", "Symbol ticker",
                     "Exchange", "Segment", "Scrip code", "Underlying symbol", "Underlying scrip code", "Strike price",
                     "Option type", "Underlying FyToken", "Reserved column", "Reserved column", "Reserved column"]

    inst_data = pd.DataFrame(columns=columns_names)
    try:
        for inst_url_record_index, inst_url_record in inst_urls.iterrows():
            segment_url_data = pd.read_csv(inst_url_record.urls)
            segment_url_data.columns = columns_names
            inst_data = pd.concat([inst_data, segment_url_data])
            inst_data.reset_index
        inst_data.to_csv(INSTRUMENTS_DATA_FILE, index=False)
    except Exception as exception:
        cus_logger.error(exception)
    cus_logger.info('Downloading instrument token had completed')
