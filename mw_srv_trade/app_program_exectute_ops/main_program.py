import os
import shutil
import sys
from datetime import datetime, timedelta
from mw_srv_trade.app_common_ops.app_common_inst_down_filter_read_write_ops import *
from mw_srv_trade.app_common_ops.app_common_ops_auto_inputs import *
from mw_srv_trade.app_common_ops.app_common_trade_user_read_write import *
from mw_srv_trade.app_common_ops.inst_trade_selection_update_ticks_indi import *
from mw_srv_trade.app_inst_mkt_data_ops.inst_mkt_data_feed_fetch_ops import generate_historical_data
from mw_srv_trade.app_inst_model_builder_ops.model_initiator import model_indicator_data_generator
from mw_srv_trade.app_model_strat_deci_mak_ops.strategy_maker import inst_mod_str_gen_ord_deci_mak
from mw_srv_trade.app_model_strat_ord_deci_ops.app_mod_str_option_order_mgt_ops import *
from mw_srv_trade.app_model_strat_user_ord_deci_ops.app_mod_str_user_ord_mgt_ops import *
from mw_srv_trade.app_msg_notify_chans_ops.app_mod_str_opt_tel_msg_ops import inst_mod_str_opt_tel_ord_deci_mak

cus_logger.setLevel(10)


def strategy_execution_steps(auto_inputs):
    """
    New instrument data will be added, and technical values will be generated on top of it; recently,
    the order management process will begin.
    """
    # user_account_balance()
    generate_historical_data(auto_inputs)
    model_indicator_data_generator(auto_inputs)
    inst_mod_str_gen_ord_deci_mak(auto_inputs)
    inst_model_strategy_option_order_decision_maker(auto_inputs)
    inst_mod_str_opt_tel_ord_deci_mak(auto_inputs)
    inst_mod_str_opt_user_order_deci_mak(auto_inputs)
    # write_user_positions()


def remove_create_dir():
    """
    All files such as order, user order, and ticks will be deleted.
    """
    # folders = [ORDERS_FOLDER_, TICKS_FOLDER_, USER_ORDERS_FOLDER_, USER_ORDERS_POSITIONS_]
    folders = [TICKS_FOLDER_]
    cus_logger.info('The process of deleting old files has begun.')
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            os.remove(file_path)


def backup_logfiles():
    all_log_files = ['geckodriver.log', 'fyersApi.log', 'mw-ved-trade.log', ]
    current_date = datetime.now().strftime("%Y-%m-%d")
    for log_file in all_log_files:
        try:
            src_path = os.path.join('', log_file)
            dst_path = os.path.join('log_backup/' + current_date, log_file)
            os.makedirs('log_backup/' + current_date, exist_ok=True)
            shutil.move(src_path, dst_path)
        except Exception as exception:
            print(f"Error moving file '{log_file}': {str(exception)}")


def execute_strategy_programs():
    """
    A user session token will be generated, and the most recent instruments file will be downloaded and saved to the
    local directory.
    """
    cus_logger.info("strategy execution started")
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info = user_info[user_info.day != date.today().day]
    if user_info.shape[0] > 0:
        backup_logfiles()
        download_each_user_tokens()
        download_write_instrument_tokens()
        calculate_expiry_date()
        ticks_indi_file_update()
        ticks_indi_file_qty_update()
        remove_create_dir()

    strategy_execution_steps(auto_inputs)
    cus_logger.info("strategy execution completed")


def scheduler_main_program_run(env, minutes, super_trend_period, super_trend_multiplier):
    """
    This function will update the input parameters and launch the main programme.
    """
    try:
        cus_logger.info("%s main program execution started", env)
        update_auto_inputs(env, minutes, super_trend_period, super_trend_multiplier)
        execute_strategy_programs()
        cus_logger.info("%s main program execution ended", env)
        sys.exit()
    except Exception as exception:
        cus_logger.exception(exception)
