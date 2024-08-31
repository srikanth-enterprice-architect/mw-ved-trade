from mw_srv_trade.app_common_trade_lib.strategy_builder.strategy_builder import strategy_data_builder_
from mw_srv_trade.app_inst_mkt_data_ops.inst_mkt_data_feed_fetch_ops import *

cus_logger.setLevel(10)


def model_indicator_data_generator(auto_inputs):
    """
    will generate the technical values for inputted instrument data
    """
    ticks_indicator_df = trade_ready_instruments_df()
    columns = ['instrument_trading_symbol', 'instrument_name']
    ticks_indicator_df = ticks_indicator_df[columns].drop_duplicates(subset=['instrument_name'])
    for record_position, indicator_record in ticks_indicator_df.iterrows():
        mk_status = market_status(indicator_record.instrument_name)
        if mk_status:
            instrument_history_data = read_data_file(indicator_record.instrument_trading_symbol.replace(':', '_'),
                                                     TICKS_FOLDER_, auto_inputs['data_interval'][0])

            instrument_history_data = strategy_data_builder_(instrument_history_data.copy(), auto_inputs,
                                                             indicator_record.instrument_trading_symbol)

            write_data_file(str(indicator_record.instrument_trading_symbol.replace(':', '_')),
                            instrument_history_data, TICKS_FOLDER_, str(auto_inputs['data_interval'][0]))
