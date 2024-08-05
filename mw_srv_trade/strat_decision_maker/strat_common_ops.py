import pandas as pd

from mw_srv_trade.comm_ops.common_ops import multi_order_qty_normal_order
from mw_srv_trade.constants.file_constants import AUTO_INPUTS_FILE
from mw_srv_trade.instr_ops.instrument_read_write_operations import read_instrument_tokens
from mw_srv_trade.msg_notify_chans.tg_msg_chan_builder import send_to_telegram
from mw_srv_trade.ord_mgmt.order_management__ce_pe import place_instrument_orders
from mw_srv_trade.trade_logger.logger import cus_logger


def inst_orders_filtered(inst_order_file_name, inst_record, strategy_name):
    inst_order_data = pd.read_csv(inst_order_file_name)
    inst_order_data_filtered = inst_order_data[inst_order_data.inst_strategy == strategy_name]
    inst_order_data_filtered = inst_order_data_filtered[
        inst_order_data_filtered.inst_name == inst_record.instrument_trading_symbol]
    return inst_order_data_filtered


def inst_exit_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record, inst_strategy_data,
                           inst_name, sp_user_session, strategy_name):
    if 'up' in inst_last_record_dir:
        ind_last_record_value = 'up_exit'
        inst_order_preparation(file_exists, ind_last_record_value, inst_order_file_name, inst_record,
                               inst_strategy_data, inst_name, sp_user_session, strategy_name)
    elif 'down' in inst_last_record_dir:
        ind_last_record_value = 'down_exit'
        inst_order_preparation(file_exists, ind_last_record_value, inst_order_file_name, inst_record,
                               inst_strategy_data, inst_name, sp_user_session, strategy_name)


def inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record, inst_strategy_data,
                            inst_name, sp_user_session, strategy_name):
    if 'up' in inst_last_record_dir:
        ind_last_record_value = 'up_entry'
        inst_order_preparation(file_exists, ind_last_record_value, inst_order_file_name, inst_record,
                               inst_strategy_data, inst_name, sp_user_session, strategy_name)
    elif 'down' in inst_last_record_dir:
        ind_last_record_value = 'down_entry'
        inst_order_preparation(file_exists, ind_last_record_value, inst_order_file_name, inst_record,
                               inst_strategy_data, inst_name, sp_user_session, strategy_name)


def inst_order_preparation(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                           inst_strategy_data, inst_name, sp_user_session, strategy_name):
    cus_logger.info('Instrument(%s) order type (%s) available', inst_name, inst_last_record_dir)
    strategy_builder_orders = pd.DataFrame()
    if file_exists and 'exit' in inst_last_record_dir:
        inst_order_record = create_inst_order_record(inst_record, inst_last_record_dir,
                                                     inst_strategy_data, inst_name, strategy_name,
                                                     inst_order_file_name, sp_user_session)
        inst_order_record_ = pd.concat([strategy_builder_orders, inst_order_record], ignore_index=True)
        inst_order_data = pd.read_csv(inst_order_file_name)
        inst_order_data = pd.concat([inst_order_data, inst_order_record_.tail(1)], ignore_index=True)
        inst_order_data.to_csv(inst_order_file_name, index=False)
        #auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
        #send_to_telegram(inst_record, inst_last_record_dir, sp_user_session, inst_order_record, strategy_name)
        #place_instrument_orders(auto_inputs, inst_record)
        cus_logger.info('appended the new position order into the file')
    elif (not file_exists) and ('entry' in inst_last_record_dir):
        inst_order_record = create_inst_order_record(inst_record, inst_last_record_dir,
                                                     inst_strategy_data, inst_name, strategy_name,
                                                     inst_order_file_name, sp_user_session)

        strategy_builder_orders = pd.concat([strategy_builder_orders, inst_order_record], ignore_index=True)
        strategy_builder_orders.to_csv(inst_order_file_name, index=False)
        #auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
        #send_to_telegram(inst_record, inst_last_record_dir, sp_user_session, inst_order_record, strategy_name)
        #place_instrument_orders(auto_inputs, inst_record)
        cus_logger.info('created new position order file')


def create_inst_order_record(ind_record, inst_last_record_dir, inst_strategy_data, inst_name,
                             strategy_name, inst_order_file_name, sp_user_session):
    quote_info = (sp_user_session.quotes({"symbols": inst_name}))['d'][0]['v']
    future_price_last_price = quote_info['lp']
    instrument_details = read_instrument_tokens(ind_record.instrument_name.split(':')[1], quote_info,
                                                inst_last_record_dir, ind_record.instrument_name.split(':')[0],
                                                ind_record.instrument_expiry_date)
    instrument_details = instrument_details.iloc[-1]
    inst_option_name = instrument_details['Symbol ticker']
    inst_option_type = instrument_details['Option type']

    if 'exit' in inst_last_record_dir:
        order_file = pd.read_csv(inst_order_file_name)
        order_file_ = order_file[order_file.inst_strategy == strategy_name].iloc[-1]
        inst_option_name = order_file_.inst_option_name
        inst_option_type = order_file_.inst_option_type
    multi_order_qty_ = multi_order_qty_normal_order(ind_record)
    return pd.DataFrame([{'inst_date': inst_strategy_data.iloc[1].date, 'inst_name': inst_name,
                          'inst_strategy': strategy_name, 'inst_price': future_price_last_price,
                          'inst_option_name': inst_option_name, 'inst_option_type': inst_option_type,
                          'inst_qty': multi_order_qty_, 'inst_direction': inst_last_record_dir,
                          'inst_exchange': ind_record.instrument_name.split(':')[0],
                          'inst_expiry_date': ind_record.instrument_expiry_date}])
