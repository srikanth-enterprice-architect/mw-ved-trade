USER_ORDERS_FOLDER_ = 'resources/account_data/userOrders/'
USER_ORDERS_POSITIONS_ = 'resources/account_data/positions/'
TICKS_FOLDER_ = 'resources/account_data/ticks/'
ORDERS_FOLDER_ = 'resources/account_data/orders/'
INSTRUMENTS_DATA_FILE = 'resources/instruments/data.csv'
ACCOUNTS_FOLDER = 'resources/account_data/account/'
GS_SHEET_KEYS = 'resources/keys/keys.json'
AUTO_INPUTS_FILE = f'{ACCOUNTS_FOLDER}auto_inputs.csv'
USER_INPUTS_FILE = f'{ACCOUNTS_FOLDER}user_info.csv'
TICKS_IND_FILE = f'{ACCOUNTS_FOLDER}ticks_indi.csv'
FIREFOX_DRIVER_PATH = 'resources/project_files/geckodriver.exe'
CLIENT_CAPITAL = 'resources/telegram/client_capital.csv'
DAY_INSTRUMENT_ORDERS = 'resources/telegram/day_instrument_orders.csv'
DATA_BACK_UP_FOLDER = 'resources/account_data/data_backup'


def create_indicator_order_file(instrument_token, data_interval):
    return ORDERS_FOLDER_ + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv '


def create_indicator_user_order_file(user_id, instrument_token, data_interval):
    return USER_ORDERS_FOLDER_ + str(user_id) + '_' + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv'
