from datetime import datetime
import pandas as pd

from mw_minisoft.trade_lib.strategy_builder.strategy_builder_common import exit_entry_time, collect_day_specific_data, \
    instrument_day_orders
from mw_minisoft.trade_lib.strategy_builder.strategy_formulas import first_candle_type_input_build
from mw_minisoft.trade_logger.logger import cus_logger


def strategy_builder_hill_base_entry(instr_days_data, instr_days, instrument_name):
    instr_days_data['hill_base_direction'] = None
    exit_time_ = exit_entry_time(instrument_name)
    num_trades = 2
    for instr_days_record_position, instr_days_record in instr_days.iloc[1:].iterrows():
        instr_day_data = collect_day_specific_data(instr_days_data, instr_days_record)
        current_day = instr_days_record.days
        previous_day = instr_days.iloc[instr_days_record_position - 1].days
        current_candle_dict_info = {'date': current_day}
        current_candle_dict_info = first_candle_type_input_build(instr_days_data, current_day, previous_day,
                                                                 current_candle_dict_info)

        instr_day_orders = pd.DataFrame()
        instr_positional_orders = pd.DataFrame()
        for row_number, row_record in instr_day_data.iloc[1:].iterrows():
            current_time = row_record.date.time()
            exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
            current_record_direction = instr_days_data.iloc[row_number].super_trend_direction_7_1
            previous_record_direction = instr_days_data.iloc[row_number - 1].super_trend_direction_7_1
            instr_day_orders = instrument_day_orders(instr_day_orders, row_record, current_record_direction,
                                                     previous_record_direction)

            if instr_day_orders.shape[0] > num_trades:
                first_record = instr_day_orders.iloc[instr_day_orders.shape[0] - 3].close
                if instr_positional_orders.shape[0] == 0:
                    if current_record_direction != previous_record_direction:
                        second_record = instr_day_orders.iloc[instr_day_orders.shape[0] - 2].close
                        third_record = instr_day_orders.iloc[instr_day_orders.shape[0] - 1].close
                        if current_candle_dict_info[
                            'candle_position'] != 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' and current_record_direction == 'up' and third_record > second_record or \
                                current_candle_dict_info[
                                    'candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' and current_record_direction == 'up' and third_record < second_record:
                            instr_days_data.loc[row_number, 'hill_base_direction'] = 'up_entry'
                            instr_positional_orders = instr_positional_orders.append(
                                instr_days_data.loc[row_number],
                                ignore_index=True)

                        elif (current_candle_dict_info[
                                  'candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction != 'up') and (
                                current_candle_dict_info[
                                    'candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction != 'down' or third_record < second_record) and (
                                current_candle_dict_info[
                                    'candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction == 'down') and (
                                current_candle_dict_info[
                                    'candle_position'] != 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction != 'up') and (
                                current_candle_dict_info[
                                    'candle_position'] != 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction != 'down' or third_record > second_record) and (
                                current_candle_dict_info[
                                    'candle_position'] != 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE' or current_record_direction == 'down'):
                            instr_days_data.loc[row_number, 'hill_base_direction'] = 'down_entry'
                            instr_positional_orders = instr_positional_orders.append(
                                instr_days_data.loc[row_number],
                                ignore_index=True)

                elif (instr_positional_orders.shape[0] == 1) & (current_time < exit_time):
                    day_first_orders = instr_positional_orders.loc[0]
                    if 'up' in str(day_first_orders.hill_base_direction):
                        if row_record.close < day_first_orders.close - 400:
                            instr_days_data.loc[row_number, 'hill_base_direction'] = 'up_exit'
                            instr_positional_orders = instr_positional_orders.append(instr_days_data.loc[row_number],
                                                                                     ignore_index=True)
                        else:
                            instr_days_data.loc[row_number, 'hill_base_direction'] = instr_days_data.loc[
                                row_number - 1, 'hill_base_direction']

                    elif 'down' in str(day_first_orders.hill_base_direction):
                        if row_record.close > (day_first_orders.close + 400):
                            instr_days_data.loc[row_number, 'hill_base_direction'] = 'down_exit'
                            instr_positional_orders = instr_positional_orders.append(instr_days_data.loc[row_number],
                                                                                     ignore_index=True)
                        else:
                            instr_days_data.loc[row_number, 'hill_base_direction'] = instr_days_data.loc[
                                row_number - 1, 'hill_base_direction']

                elif (instr_positional_orders.shape[0] == 1) & (current_time >= exit_time):
                    day_first_orders = instr_positional_orders.loc[0]
                    if 'up' in str(day_first_orders.hill_base_direction):
                        instr_days_data.loc[row_number, 'hill_base_direction'] = 'up_exit'
                        instr_positional_orders = instr_positional_orders.append(instr_days_data.loc[row_number],
                                                                                 ignore_index=True)
                    elif 'down' in str(day_first_orders.hill_base_direction):
                        instr_days_data.loc[row_number, 'hill_base_direction'] = 'down_exit'
                        instr_positional_orders = instr_positional_orders.append(instr_days_data.loc[row_number],
                                                                                 ignore_index=True)

                # Enable -> same -> Double Order
                if instr_day_orders.shape[0] == 2:
                    # instr_day_orders = pd.DataFrame()
                    cus_logger.info("strategy_builder_hill_base_entry execution    ---    ended")

    return instr_days_data
