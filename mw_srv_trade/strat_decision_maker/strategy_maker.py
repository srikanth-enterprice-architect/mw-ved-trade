from mw_srv_trade.comm_ops.common_ops import super_user_session
from mw_srv_trade.persist_ops.account_management import ticks_indi, market_status
from mw_srv_trade.strat_decision_maker.day_open_strategy import inst_strategy_dos_execution
from mw_srv_trade.strat_decision_maker.super_log_buy_sell_strat import strategy_order_decision_maker
from mw_srv_trade.trade_logger.logger import cus_logger


def storage_regular_orders(auto_inputs):
    """
    This code will create each new orders in the file by comparing the previous two records generated by the
    super-trend indicator.
    """
    instruments_df = ticks_indi()
    sp_user_session, sp_user_record = super_user_session()
    for inst_record_position, inst_record in instruments_df.iterrows():
        ticks = inst_record.instrument_trading_symbol.replace(':', '_')
        if inst_record.avail == 'Y' and market_status(ticks):
            strategy_name = inst_record.start_name
            if inst_record.start_name in ["super_long_buy_side", "super_long_sell_side"]:
                strategy_order_decision_maker(auto_inputs, inst_record, sp_user_session, strategy_name)
            elif inst_record.start_name == "day_open_strategy":
                inst_strategy_dos_execution(auto_inputs, inst_record, sp_user_session, strategy_name)

    cus_logger.info('storing regular orders function is completed')
