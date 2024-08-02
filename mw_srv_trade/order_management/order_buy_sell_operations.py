import pandas as pd
from mw_srv_trade.common_operatinos.common_ops import multi_order_qty_normal_original
from mw_srv_trade.constants.file_constants import AUTO_INPUTS_FILE
from mw_srv_trade.trade_logger.logger import cus_logger


def user_position_enter(inst_order_last_record, inst_record, user_record, user_session):
    env = pd.read_csv(AUTO_INPUTS_FILE).iloc[0].env
    if (env == 'prod') and ('entry' in str(inst_order_last_record.inst_direction)):
        trading_symbol = inst_order_last_record.inst_option_name
        data = {"symbol": trading_symbol, "ohlcv_flag": "1"}
        price_last, order_info = 0, "order info not available"
        order_qty = multi_order_qty_normal_original(inst_record, user_record.user_id)
        market_type = 'INTRADAY'  # "INTRADAY"
        try:
            price_last = (user_session.depth(data)['d'][trading_symbol]['ask'])[4]['price']
        except Exception as exception:
            cus_logger.error(str(exception))
        cus_logger.info('Entering into new position :- instrument_token: %s ,User( %s) ,  Order Type : buy order , '
                        'ticks_ind_running_qt: %s , price: %s ', trading_symbol, user_record.user_id,
                        order_qty, price_last)
        order_input = {"symbol": trading_symbol, "qty": order_qty, "type": 1, "side": 1, "productType": market_type,
                       "limitPrice": price_last, "stopPrice": 0, "validity": "DAY", "disclosedQty": 0,
                       "offlineOrder": False}
        try:
            order_info = user_session.place_order(order_input)
            cus_logger.info(order_input)
        except Exception as exception:
            cus_logger.error(str(exception))
        cus_logger.info('Instrument Order Detail, order info %s', str(order_info))


def user_position_exit(inst_order_last_record, inst_record, user_record, user_session):
    env = pd.read_csv(AUTO_INPUTS_FILE).iloc[0].env
    if (env == 'prod') and ('exit' in str(inst_order_last_record.inst_direction)):
        trading_symbol = inst_order_last_record.inst_option_name
        data = {"symbol": trading_symbol, "ohlcv_flag": "1"}
        price_last, order_info = 0, "order info not available"
        order_qty = multi_order_qty_normal_original(inst_record, user_record.user_id)
        market_type = 'INTRADAY'  # "INTRADAY"
        try:
            price_last = (user_session.depth(data)['d'][trading_symbol]['ask'])[4]['price']
        except Exception as exception:
            cus_logger.error(str(exception))
        cus_logger.info('Exiting position :- instrument_token: %s ,User( %s) ,  Order Type : buy order , '
                        'ticks_ind_running_qt: %s , price: %s ', trading_symbol, user_record.user_id, order_qty,
                        price_last)
        order_input = {"symbol": trading_symbol, "qty": order_qty, "type": 1, "side": -1, "productType": market_type,
                       "limitPrice": price_last, "stopPrice": 0, "validity": "DAY", "disclosedQty": 0,
                       "offlineOrder": False}
        try:
            order_info = user_session.place_order(order_input)
            cus_logger.info(order_input)
        except Exception as exception:
            cus_logger.error(str(exception))
        cus_logger.info('Instrument Order Detail, order info %s', str(order_info))
