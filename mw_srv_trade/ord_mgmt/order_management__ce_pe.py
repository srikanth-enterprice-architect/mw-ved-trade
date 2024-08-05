from os import path

from mw_srv_trade.ord_mgmt.order_buy_sell_operations import *
from mw_srv_trade.persist_ops.account_management import *
from mw_srv_trade.trade_lib.session_builder.retrive_request_token import generate_user_session

cus_logger.setLevel(10)


def place_instrument_orders(auto_inputs, inst_record):
    """
    This code will determine whether an existing indicator order file is available and, if so, will create a user
    order file and place a new order for each and every user.
    """

    inst_token = inst_record.instrument_trading_symbol.replace(':', '_')
    inst_data_interval = auto_inputs['data_interval'][0]
    inst_order_file_name = create_indicator_order_file(inst_token, inst_data_interval)
    if path.exists(inst_order_file_name):
        inst_order_record = pd.read_csv(inst_order_file_name)
        inst_order_record = inst_order_record[inst_order_record.inst_strategy == inst_record.start_name].tail(1)
        user_records = pd.DataFrame(read_user_info())
        for user_record_position, user_record in user_records.iterrows():
            try:
                place_instrument_user_orders(auto_inputs, inst_order_record, inst_record, user_record)
            except Exception as all_errors:
                cus_logger.error("User (%s) Entering into new order Instrument position (%s) had been failed - "
                                 "error message %s", user_record.user_id, inst_record.instrument_name, all_errors)
    else:
        cus_logger.info('fresh order is not available')


def place_instrument_user_orders(auto_inputs, inst_order_record, inst_record, user_record):
    """
    This code will determine whether an existing user indicator order file is available and, if available will append
    nor create new file then place order
    """
    inst_token = inst_record.instrument_trading_symbol.replace(':', '_')
    inst_data_interval = auto_inputs['data_interval'][0]
    user_id = user_record['user_id']
    inst_user_order_file_name = create_indicator_user_order_file(user_id, inst_token, inst_data_interval)
    if path.exists(inst_user_order_file_name):
        inst_user_orders_data = pd.read_csv(inst_user_order_file_name)
        inst_user_orders_data_ = inst_user_orders_data[
            inst_user_orders_data['inst_strategy'] == inst_order_record.iloc[-1]['inst_strategy']]
        inst_user_orders_filtered_ = inst_user_orders_data_[
            inst_user_orders_data_['inst_option_name'] == inst_order_record.iloc[-1]['inst_option_name']]
        inst_user_orders_last_record = inst_user_orders_filtered_[
            inst_user_orders_filtered_['inst_direction'] == inst_order_record.iloc[-1]['inst_direction']]
        if inst_user_orders_last_record.shape[0] > 0:
            inst_user_orders_last_record = inst_user_orders_last_record.tail(1)
            inst_user_orders_last_record_date = pd.to_datetime(inst_user_orders_last_record.iloc[-1]['inst_date'])
            inst_order_last_record_date = pd.to_datetime(inst_order_record.iloc[-1].inst_date)
            diff = (inst_user_orders_last_record_date - inst_order_last_record_date).total_seconds() / 60
            if diff != 0:
                cus_logger.info('initiating the process to initiate the position order')
                inst_user_order_data = pd.read_csv(inst_user_order_file_name)
                (inst_user_order_data.append(inst_order_record)).to_csv(inst_user_order_file_name, index=False)
                place_instrument_user_orders_based_position(inst_order_record, inst_record, user_record)
        else:
            cus_logger.info('initiating the process to initiate the new position order')
            (inst_user_orders_data.append(inst_order_record)).to_csv(inst_user_order_file_name, index=False)
            place_instrument_user_orders_based_position(inst_order_record, inst_record, user_record)
    else:
        cus_logger.info('initiating the process to initiate the new position order')
        inst_order_record.to_csv(inst_user_order_file_name, index=False)
        place_instrument_user_orders_based_position(inst_order_record, inst_record, user_record)


def place_instrument_user_orders_based_position(inst_order_last_record, inst_record, user_record):
    """
    This code will check the user's position and, based on that, will either enter a new position or exit an existing
    position and enter a new one.
    """
    inst_order_last_record = inst_order_last_record.iloc[0]
    user_session = generate_user_session(user_record)
    env = pd.read_csv(AUTO_INPUTS_FILE).iloc[0].env
    user_net_positions = pd.DataFrame(user_session.positions()['netPositions'])
    if env == 'test':
        user_net_positions = pd.read_csv('resources/account/net_positions.csv')
    cus_logger.info('checking existing position is available or not')
    if user_net_positions.shape[0] > 0 and user_net_positions[user_net_positions['qty'] != 0].shape[0] > 0:
        user_net_positions = user_net_positions[user_net_positions.symbol == inst_order_last_record.inst_option_name]
        if (user_net_positions.shape[0] > 0) and ('exit' in str(inst_order_last_record.inst_direction)):
            cus_logger.info('clearing existing positions %s As holding quantity',
                            inst_order_last_record.inst_option_name)
            user_position_exit(inst_order_last_record, inst_record, user_record, user_session)
        elif (user_net_positions.shape[0] == 0) and ('entry' in str(inst_order_last_record.inst_direction)):
            cus_logger.info('Entering into instrument position %s As quantity zero',
                            inst_order_last_record.inst_option_name)
            user_position_enter(inst_order_last_record, inst_record, user_record, user_session)
    else:
        cus_logger.info('Entering into new instrument position %s As quantity zero',
                        inst_order_last_record.inst_option_name)
        user_position_enter(inst_order_last_record, inst_record, user_record, user_session)
