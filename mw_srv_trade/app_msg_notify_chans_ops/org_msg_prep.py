from datetime import datetime
import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *
from mw_srv_trade.app_msg_notify_chans_ops.tg_msg_chan_builder import sent_telegram_message


def inst_mod_str_tel_msg_entry_ord(inst_opt_ord_global_obj, sp_user_ses):
    client_capital_book = pd.read_csv(CLIENT_CAPITAL)
    day_instrument_orders = pd.DataFrame()
    if inst_opt_ord_global_obj['tel_ord_file_exits']:
        day_instrument_orders = pd.read_csv(INST_MOD_STR_OPT_ORD_TEL_MSG_FILE)
    inst_opt_ord_new = inst_opt_ord_global_obj['inst_mod_str_opt_ord_flt_exit_'].iloc[-1]
    client_capital, current_capital = client_capital_info(client_capital_book)
    multi_order_qty_ = multi_order_qty_normal_instagram(inst_opt_ord_global_obj)
    instrument_entry_price = inst_opt_ord_new.inst_option_entry_price
    current_capital = current_capital - (multi_order_qty_ * instrument_entry_price)
    col_name = ['traded date', 'investment amount', 'current amount', 'instrument buy name', 'instrument entry type',
                'instrument option name', 'instrument buy price', 'option buy name', 'option buy price',
                'option buy qty', 'option buy time', 'instrument sell name', 'instrument exit type',
                'instrument sell price', 'option sell name', 'option sell qty', 'option sell time', 'option sell price',
                'option profit or loss', 'strategy name']
    col_val = [[inst_opt_ord_new.inst_date, client_capital, current_capital, inst_opt_ord_new.inst_name,
                inst_opt_ord_new.inst_entry_type, inst_opt_ord_new.inst_opt_sym_name, inst_opt_ord_new.inst_entry_price,
                inst_opt_ord_new.inst_option_entry_type, inst_opt_ord_new.inst_option_entry_price, multi_order_qty_,
                inst_opt_ord_new.inst_option_entry_time, inst_opt_ord_new.inst_name, inst_opt_ord_new.inst_exit_type,
                inst_opt_ord_new.inst_exit_price, inst_opt_ord_new.inst_option_exit_type, multi_order_qty_,
                inst_opt_ord_new.inst_option_exit_time, inst_opt_ord_new.inst_option_exit_price,
                inst_opt_ord_new.inst_option_profit, inst_opt_ord_new.strategy_name]]
    inst_mod_str_tel_msg_entry_ord_df = pd.DataFrame(col_val, columns=col_name)
    day_instrument_orders = pd.concat([day_instrument_orders, inst_mod_str_tel_msg_entry_ord_df], ignore_index=True)
    client_capital_book.at[0, 'client_current_capital'] = current_capital
    client_capital_book.to_csv(CLIENT_CAPITAL, index=False)
    sent_telegram_message(inst_mod_str_tel_msg_entry_ord_df.iloc[-1])
    day_instrument_orders.to_csv(INST_MOD_STR_OPT_ORD_TEL_MSG_FILE, index=False)



def inst_mod_str_tel_msg_exit_ord(inst_opt_ord_global_obj, sp_user_ses):
    inst_opt_ord_new = inst_opt_ord_global_obj['inst_model_str_tel_ord_file_exit_']
    inst_tel_opt_ords = inst_opt_ord_global_obj['inst_model_str_opt_tel_ord_file_df']
    inst_tel_opt_ords_flt = inst_opt_ord_global_obj['inst_model_str_tel_ord_file_exit_']
    inst_entry_qty, client_current_capital, inst_profit_loss = inst_tel_opt_cap_cal(inst_opt_ord_new, sp_user_ses)
    inst_opt_ord_new = inst_opt_ord_new.iloc[-1]
    day_inst_orders_index = inst_tel_opt_ords.shape[0]
    option_sell_time = datetime.now().time().strftime('%H:%M:%S')
    if inst_tel_opt_ords_flt.size > 0:
        day_inst_orders_index = inst_tel_opt_ords_flt.index.values[0]
    col_name = ['instrument sell name', 'instrument exit type', 'instrument sell price', 'option sell name',
                'option sell qty', 'option sell time', 'option sell price', 'current amount', "option profit or loss"]
    col_val = [inst_opt_ord_new['instrument sell name'], inst_opt_ord_new['instrument exit type'],
               inst_opt_ord_new['instrument sell price'], inst_opt_ord_new['option buy name'],
               inst_opt_ord_new['option buy qty'], option_sell_time, inst_opt_ord_new['option sell price'],
               client_current_capital, inst_profit_loss]
    inst_tel_opt_ords.at[day_inst_orders_index, col_name] = col_val
    inst_tel_opt_ords.to_csv(INST_MOD_STR_OPT_ORD_TEL_MSG_FILE, index=False)
    sent_telegram_message(inst_tel_opt_ords.iloc[day_inst_orders_index])


def inst_tel_opt_cap_cal(inst_opt_ord_new, sp_user_ses):
    ind_opt_ord_new = inst_opt_ord_new.index.values[0]
    inst_option_exit_type = inst_opt_ord_new['option buy name'].values[0]
    instrument_buy_name = inst_opt_ord_new['instrument buy name'].values[0]
    inst_option_exit_price = sp_user_ses.quotes({"symbols": inst_option_exit_type})['d'][0]['v']['ask']
    instrument_sell_price = sp_user_ses.quotes({"symbols": instrument_buy_name})['d'][0]['v']['lp']
    inst_exit_type = inst_opt_ord_new['instrument entry type'].values[0].replace('entry', 'exit')
    inst_sell_name = inst_opt_ord_new['instrument buy name'].values[0]
    col_names = ['option sell price', 'instrument sell price', 'instrument exit type', 'instrument sell name']
    col_val = [inst_option_exit_price, instrument_sell_price, inst_exit_type, inst_sell_name]
    inst_opt_ord_new.loc[ind_opt_ord_new, col_names] = col_val
    client_capital_book = pd.read_csv(CLIENT_CAPITAL)
    client_capital_book_record = client_capital_book.iloc[-1]
    inst_entry_qty = inst_opt_ord_new['option buy qty'].values[0]
    client_current_capital = client_capital_book_record.client_current_capital
    client_current_capital = client_current_capital + (inst_entry_qty * inst_option_exit_price)
    profit_loss = (inst_opt_ord_new['option buy price'].values[0] - inst_option_exit_price)
    inst_profit_loss = -(profit_loss * inst_entry_qty)

    update_client_capital_book(client_capital_book, client_current_capital)
    return inst_entry_qty, client_current_capital, inst_profit_loss


def update_client_capital_book(client_capital_book, client_current_capital):
    client_capital_book.at[0, 'client_current_capital'] = client_current_capital
    client_capital_book.to_csv(CLIENT_CAPITAL, index=False)


def inst_tel_opt_ord_flt(inst_opt_ord_global_obj):
    inst_tel_opt_ords = pd.read_csv(INST_MOD_STR_OPT_ORD_TEL_MSG_FILE)
    inst_opt_ord_new = inst_opt_ord_global_obj['inst_model_str_tel_ord_file_exit_'].iloc[-1]
    inst_option_entry_type = inst_opt_ord_new.inst_option_entry_type
    strategy_name = inst_opt_ord_new.strategy_name
    inst_tel_opt_ords_flt = inst_tel_opt_ords[inst_tel_opt_ords['option buy name'] == inst_option_entry_type]
    inst_tel_opt_ords_flt = inst_tel_opt_ords_flt[inst_tel_opt_ords_flt['strategy name'] == strategy_name]
    inst_tel_opt_ords_flt = inst_tel_opt_ords_flt[inst_tel_opt_ords_flt['option sell name'].isna()].tail(1)
    return inst_tel_opt_ords, inst_tel_opt_ords_flt


def client_capital_book_preparation(client_capital_book, inst_record, inst_order_record, sp_user_session):
    instrument_exit_price = common_params(sp_user_session, inst_order_record)
    client_current_capital = client_capital_book.iloc[-1].client_current_capital
    client_current_capital = client_current_capital + (inst_record.default_quantity * instrument_exit_price)
    client_capital_book.at[0, 'client_current_capital'] = client_current_capital


def common_params(sp_user_session, inst_order_record):
    inst_name = inst_order_record['inst_option_name']
    inst_data = {"symbols": inst_name}
    return (sp_user_session.quotes(inst_data))['d'][0]['v']['lp']


def client_capital_info(client_capital_book):
    client_capital = client_capital_book.iloc[-1].client_capital
    current_capital = client_capital_book.iloc[-1].client_current_capital
    return client_capital, current_capital


def multi_order_qty_normal_instagram(inst_opt_ord_global_obj):
    ind_record = inst_opt_ord_global_obj['trade_ready_inst_record']
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
    multi_order_qty_ = multi_order_qty_ * ind_record.telegram_qty
    return multi_order_qty_
