import os

import pandas as pd

from mw_srv_trade.trade_logger.logger import cus_logger


def write_data_file(instrument_token, instrument_data, resources_path, interval):
    instrument_data.to_csv(resources_path + str(instrument_token) + '_' + str(interval) + '.csv', index=False)


def read_data_file(instrument_token, resources_path, interval):
    file_name = resources_path + str(instrument_token) + '_' + str(interval) + '.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    else:
        cus_logger.error('File reading Failed -  %s', file_name)
