import os
import sys

from mw_srv_trade.ord_mgmt.order_management__ce_pe import *
from mw_srv_trade.persist_ops.account_management import *


def user_positions(holding_position_symbol):
    user_records = pd.read_csv(USER_INPUTS_FILE)
    for user_record_position, user_record in user_records.iterrows():
        try:
            user_session = generate_user_session(user_record)
            user_position = pd.DataFrame(user_session.positions()['netPositions'])
            user_position = user_position[user_position['netQty'] != 0]
            user_position = user_position[user_position['symbol'] == holding_position_symbol]
            price_last, order_info = 0, "order info not available"
            data = {"symbol": holding_position_symbol, "ohlcv_flag": "1"}
            if user_position.shape[0] > 0:
                try:
                    price_last = (user_session.depth(data)['d'][holding_position_symbol]['ask'])[4]['price']
                except Exception as exception:
                    cus_logger.error(str(exception))
                cus_logger.info('Exiting position :- instrument_token: %s ,User( %s) ,  Order Type : buy order , '
                                'ticks_ind_running_qt: %s , price: %s ', holding_position_symbol, user_record.user_id,
                                str(user_position.netQty), price_last)
                market_type = 'MARGIN'
                data = {"symbol": holding_position_symbol, "qty": str(user_position.netQty.values[0]), "type": 1,
                        "side": -1,
                        "productType": market_type, "limitPrice": price_last, "stopPrice": 0, "validity": "DAY",
                        "disclosedQty": 0, "offlineOrder": "False", "stopLoss": 0, "takeProfit": 0}
                try:
                    order_info = user_session.place_order(data)
                except Exception as exception:
                    cus_logger.error(str(exception))
                cus_logger.info('Instrument Order Detail, order info %s', str(order_info))
        except Exception as all_errors:
            cus_logger.error("User (%s) Entering into new order Instrument position (%s) had been failed - "
                             "error message %s", user_record.user_id, "inst_record.instrument_name", all_errors)


def user_positions_write():
    user_records = pd.read_csv(USER_INPUTS_FILE)
    for user_record_position, user_record in user_records.iterrows():
        try:
            user_session = generate_user_session(user_record)
            user_position = pd.DataFrame(user_session.positions()['netPositions'])
            user_position = user_position[user_position['netQty'] != 0]
            user_position.to_csv(
                "resources/positions/" + user_record['name'].split('.')[0] + "_" + user_record.user_id + ".csv")

        except Exception as all_errors:
            cus_logger.error("User (%s) Entering into new order Instrument position (%s) had been failed - "
                             "error message %s", user_record.user_id, "inst_record.instrument_name", all_errors)
