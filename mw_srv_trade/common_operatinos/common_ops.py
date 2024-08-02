import pandas as pd

from mw_srv_trade.constants.file_constants import FIREFOX_DRIVER_PATH
from mw_srv_trade.persistence_operations.account_management import read_user_info
from mw_srv_trade.trade_lib.session_builder.retrive_request_token import create_user_session


def super_user_session():
    user_info_df = read_user_info()
    user_info_df = user_info_df.loc[user_info_df['zerodha_datafeed'] == 'Y']
    sp_user_session, sp_user_record = create_user_session(user_info_df.loc[0], FIREFOX_DRIVER_PATH)
    return sp_user_session, sp_user_record


def multi_order_qty_normal_order(ind_record):
    ticks_indicator = pd.read_csv("resources/telegram/day_instrument_orders.csv")
    ticks_indicator = ticks_indicator[ticks_indicator['instrument name'] == ind_record.instrument_trading_symbol]
    ticks_indicator = ticks_indicator[ticks_indicator['strategy_name'] == ind_record.start_name]
    multi_order_qty_ = ind_record.default_quantity
    if ind_record.multi_quan == 'Y':
        for user_order_position, user_order in ticks_indicator.iterrows():
            if user_order['instrument profit or loss'] < 0:
                multi_order_qty_ = multi_order_qty_ + ind_record.default_quantity
            elif user_order['instrument profit or loss'] > 0:
                multi_order_qty_ = ind_record.default_quantity

    return multi_order_qty_


def multi_order_qty_normal_instagram(ind_record):
    ticks_indicator = pd.read_csv("resources/telegram/day_instrument_orders.csv")
    ticks_indicator = ticks_indicator[ticks_indicator['instrument name'] == ind_record.instrument_trading_symbol]
    ticks_indicator = ticks_indicator[ticks_indicator['strategy_name'] == ind_record.start_name]
    multi_order_qty_ = ind_record.default_quantity
    if ind_record.multi_quan == 'Y':
        for user_order_position, user_order in ticks_indicator.iterrows():
            if user_order['instrument profit or loss'] < 0:
                multi_order_qty_ = multi_order_qty_ + ind_record.default_quantity
            elif user_order['instrument profit or loss'] > 0:
                multi_order_qty_ = ind_record.default_quantity
    multi_order_qty_ = multi_order_qty_ * ind_record.telegram_qty

    return multi_order_qty_


def multi_order_qty_normal_original(ind_record, user_id):
    ticks_indicator = pd.read_csv("resources/telegram/day_instrument_orders.csv")
    ticks_indicator = ticks_indicator[ticks_indicator['instrument name'] == ind_record.instrument_trading_symbol]
    ticks_indicator = ticks_indicator[ticks_indicator['strategy_name'] == ind_record.start_name]
    multi_order_qty_ = ind_record.default_quantity
    if ind_record.multi_quan == 'Y':
        for user_order_position, user_order in ticks_indicator.iterrows():
            if user_order['instrument profit or loss'] < 0:
                multi_order_qty_ = multi_order_qty_ + ind_record.default_quantity
            elif user_order['instrument profit or loss'] > 0:
                multi_order_qty_ = ind_record.default_quantity

    return multi_order_qty_ * ind_record[user_id]
