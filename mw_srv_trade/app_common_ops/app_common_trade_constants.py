USER_ORDERS_FOLDER_ = 'resources/account_data/userOrders/'
USER_ORDERS_POSITIONS_ = 'resources/orders/inst_model_strat_option_user_positions/'
TICKS_FOLDER_ = 'resources/data/ticks/'
ORDERS_FOLDER_ = 'resources/account_data/orders/'
INSTRUMENTS_DATA_FILE = 'resources/instruments/data.csv'
ACCOUNTS_FOLDER = 'resources/account_data/'
GS_SHEET_KEYS = 'resources/keys/keys.json'
AUTO_INPUTS_FILE = f'{ACCOUNTS_FOLDER}auto_inputs.csv'
USER_INPUTS_FILE = f'{ACCOUNTS_FOLDER}user_info.csv'
TICKS_IND_FILE = f'{ACCOUNTS_FOLDER}ticks_indi.csv'
FIREFOX_DRIVER_PATH = 'resources/project_files/geckodriver.exe'
CLIENT_CAPITAL = 'resources/telegram/client_capital.csv'
INST_MOD_STR_OPT_ORD_TEL_MSG_FILE = 'resources/telegram/inst_mod_str_opt_ord_tel_msg.csv'
DATA_BACK_UP_FOLDER = 'resources/data/data_backup'
ACCOUNTS_DATA = 'resources/orders/'
INST_MODEL_STRATEGY_GEN_ORDER_DATA_FILE = f'{ACCOUNTS_DATA}inst_model_strat_gen_orders/inst_model_strat_gen_orders.csv'
INST_MODEL_STRATEGY_OPTION_ORDER_DATA_FILE = f'{ACCOUNTS_DATA}inst_model_strat_option_orders/inst_model_strat_option_orders.csv'


def create_indicator_order_file(instrument_token, data_interval):
    return ORDERS_FOLDER_ + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv '


def inst_mod_str_gen_ord_file_path_read():
    inst_model_strategy_order_file_path = INST_MODEL_STRATEGY_GEN_ORDER_DATA_FILE
    return inst_model_strategy_order_file_path


def inst_mod_str_opt_ord_file_path_read():
    inst_model_strategy_option_order_file_path = INST_MODEL_STRATEGY_OPTION_ORDER_DATA_FILE
    return inst_model_strategy_option_order_file_path


def inst_mod_str_opt_tel_ord_file_path_read():
    inst_mod_str_opt_tel_ord_file_path = INST_MOD_STR_OPT_ORD_TEL_MSG_FILE
    return inst_mod_str_opt_tel_ord_file_path


def create_indicator_user_order_file(user_id, instrument_token, data_interval):
    return USER_ORDERS_FOLDER_ + str(user_id) + '_' + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv'
