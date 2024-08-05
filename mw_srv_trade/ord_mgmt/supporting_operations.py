from mw_srv_trade.persist_ops.account_management import *
from mw_srv_trade.persist_ops.account_management import read_user_info
from mw_srv_trade.trade_lib.session_builder.retrive_request_token import generate_user_session

cus_logger.setLevel(10)


def instrument_positions(instrument_token, net_positions, nfo):
    """
    This peace of will pull the instrument raw from data.csv based on the position info
    """
    cus_logger.info('collecting the raw instrument data for order preparation')
    instruments = pd.read_csv(INSTRUMENTS_DATA_FILE, low_memory=False)

    return pd.merge(net_positions, instruments, how='inner', on=['Expiry date'])


def running_quant(instrument_trading_symbol, buy_price, current_price, apikey):
    """
    This piece of code will change the default quantity based on profit or loss, which is typically used for double
    quantities.
    """
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info_id = (user_info[user_info.api_key == apikey]).user_id.values[0]

    ticks_ind_excel = pd.read_csv(TICKS_IND_FILE)
    ticks_ind_excel_data = pd.DataFrame(ticks_ind_excel).astype(str)

    for ticks_ind_index, ticks_indicator in ticks_ind_excel_data.iterrows():
        if ticks_indicator['instrument_trading_symbol'] == instrument_trading_symbol:
            if current_price >= buy_price:
                ticks_ind_excel_data.at[ticks_ind_index, str(user_info_id)] = int(ticks_indicator.default_quantity)
            else:
                ticks_ind_excel_data.at[ticks_ind_index, str(user_info_id)] = int(
                    ticks_indicator.default_quantity) + int(ticks_indicator[user_info_id])

    ticks_ind_excel_data.to_csv(TICKS_IND_FILE, index=False)

    return ticks_ind_excel_data


def write_user_positions():
    user_info_df = pd.DataFrame(read_user_info())
    for idx_user_info, user_info in user_info_df.iterrows():
        kite_session = generate_user_session(user_info)
        net_positions = pd.DataFrame(kite_session.positions()['netPositions'])
        positions_file_name = USER_ORDERS_POSITIONS_ + str(
            user_info['name']) + '_' + user_info.user_id + '_net_positions.csv'
        net_positions.to_csv(positions_file_name)
