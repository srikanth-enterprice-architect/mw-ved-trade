from datetime import datetime
from os import path

from mw_srv_trade.constants.file_constants import create_indicator_order_file, TICKS_FOLDER_
from mw_srv_trade.mk_data_ops.data_read_write_ops import read_data_file
from mw_srv_trade.strat_decision_maker.strat_common_ops import inst_orders_filtered, inst_exit_order_method, \
    inst_entry_order_method
from mw_srv_trade.trade_lib.strategy_builder.strategy_builder_common import exit_entry_time
from mw_srv_trade.trade_logger.logger import cus_logger


def inst_strategies_execution(auto_inputs, inst_record, sp_user_session, strategy_name):
    inst_name = inst_record.instrument_trading_symbol
    inst_name_new = inst_record.instrument_trading_symbol.replace(':', '_')
    inst_order_file_name = create_indicator_order_file(inst_name_new, auto_inputs['data_interval'][0])
    cus_logger.info('checking order available for the instrument %s', inst_name)
    inst_data = read_data_file(inst_name_new, TICKS_FOLDER_, auto_inputs['data_interval'][0])
    inst_strategy_data = inst_data.tail(2)
    inst_first_record_dir = str(inst_strategy_data.iloc[0][strategy_name])
    inst_last_record_dir = str(inst_strategy_data.iloc[1][strategy_name])
    exit_time_ = exit_entry_time(inst_name)
    current_time = datetime.now().time().strftime('%H:%M:%S')
    before_mkt = current_time < exit_time_
    if inst_last_record_dir != inst_first_record_dir and before_mkt and inst_last_record_dir != 'nan':
        if file_exists := path.exists(inst_order_file_name):
            inst_order_data_filtered = inst_orders_filtered(inst_order_file_name, inst_record, strategy_name)
            if inst_order_data_filtered.shape[0] > 0:
                inst_last_order_dir = inst_order_data_filtered.iloc[-1].inst_direction
                inst_last_order_dir_ = inst_last_order_dir.split('_')[0]
                if (inst_last_order_dir_ != inst_last_record_dir) and ('entry' in inst_last_order_dir):
                    inst_exit_order_method(file_exists, inst_last_order_dir_, inst_order_file_name, inst_record,
                                           inst_strategy_data, inst_name, sp_user_session, strategy_name)

                    inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                                            inst_strategy_data, inst_name, sp_user_session, strategy_name)
            else:
                inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                                        inst_strategy_data, inst_name, sp_user_session, strategy_name)
        else:
            inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                                    inst_strategy_data, inst_name, sp_user_session, strategy_name)

    elif inst_last_record_dir == inst_first_record_dir and before_mkt and inst_last_record_dir != 'nan':
        if file_exists := path.exists(inst_order_file_name):
            inst_order_data_filtered = inst_orders_filtered(inst_order_file_name, inst_record, strategy_name)
            if inst_order_data_filtered.shape[0] > 0:
                inst_last_order_dir = inst_order_data_filtered.iloc[-1].inst_direction
                inst_last_order_dir_ = inst_last_order_dir.split('_')[0]
                if inst_last_order_dir_ != inst_last_record_dir and 'entry' in inst_last_order_dir:
                    inst_exit_order_method(file_exists, inst_last_order_dir_, inst_order_file_name, inst_record,
                                           inst_strategy_data, inst_name, sp_user_session, strategy_name)
                    inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                                            inst_strategy_data, inst_name, sp_user_session, strategy_name)

    elif current_time >= exit_time_:
        if file_exists := path.exists(inst_order_file_name):
            inst_order_data_filtered = inst_orders_filtered(inst_order_file_name, inst_record, strategy_name)
            expiry_today = datetime.strptime(inst_record['instrument_expiry_date'],
                                             '%d-%m-%Y').date() == datetime.now().date()
            if inst_order_data_filtered.shape[0] > 0 and inst_record.holding == 'N':
                inst_last_order_dir = inst_order_data_filtered.iloc[-1].inst_direction
                if 'entry' in inst_last_order_dir:
                    inst_exit_order_method(file_exists, inst_last_order_dir, inst_order_file_name, inst_record,
                                           inst_strategy_data, inst_name, sp_user_session, strategy_name)
            if inst_order_data_filtered.shape[0] > 0 and inst_record.holding == 'Y' and expiry_today:
                inst_last_order_dir = inst_order_data_filtered.iloc[-1].inst_direction
                if 'entry' in inst_last_order_dir:
                    inst_exit_order_method(file_exists, inst_last_order_dir, inst_order_file_name, inst_record,
                                           inst_strategy_data, inst_name, sp_user_session, strategy_name)

                    inst_entry_order_method(file_exists, inst_last_record_dir, inst_order_file_name, inst_record,
                                            inst_strategy_data, inst_name, sp_user_session, strategy_name)
