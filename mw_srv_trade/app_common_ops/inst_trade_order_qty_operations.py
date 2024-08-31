import pandas as pd


def multi_order_qty_normal_order(ind_record):
    ticks_indicator = pd.read_csv("resources/telegram/inst_mod_str_opt_ord_tel_msg.csv")
    ticks_indicator = ticks_indicator[ticks_indicator['instrument buy name'] == ind_record.instrument_trading_symbol]
    ticks_indicator = ticks_indicator[ticks_indicator['strategy name'] == ind_record.start_name]
    multi_order_qty_ = ind_record.default_quantity
    if ind_record.multi_quan == 'Y':
        for user_order_position, user_order in ticks_indicator.iterrows():
            if user_order['instrument profit or loss'] < 0:
                multi_order_qty_ = multi_order_qty_ + ind_record.default_quantity
            elif user_order['instrument profit or loss'] > 0:
                multi_order_qty_ = ind_record.default_quantity

    return multi_order_qty_


def multi_order_qty_normal_original(ind_record, user_id):
    ticks_indicator = pd.read_csv("resources/telegram/inst_mod_str_opt_ord_tel_msg.csv")
    ticks_indicator = ticks_indicator[ticks_indicator['instrument buy name'] == ind_record.instrument_trading_symbol]
    ticks_indicator = ticks_indicator[ticks_indicator['strategy name'] == ind_record.start_name]
    multi_order_qty_ = ind_record.default_quantity
    if ind_record.multi_quan == 'Y':
        for user_order_position, user_order in ticks_indicator.iterrows():
            if user_order['instrument profit or loss'] < 0:
                multi_order_qty_ = multi_order_qty_ + ind_record.default_quantity
            elif user_order['instrument profit or loss'] > 0:
                multi_order_qty_ = ind_record.default_quantity

    return multi_order_qty_ * ind_record[user_id]
