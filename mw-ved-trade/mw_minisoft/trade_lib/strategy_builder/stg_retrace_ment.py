from datetime import datetime
import pandas as pd

from mw_minisoft.trade_lib.strategy_builder.strategy_builder_common import collect_day_specific_data
from mw_minisoft.trade_lib.strategy_builder.strategy_formulas import first_candle_type_input_build
from mw_minisoft.trade_logger.logger import cus_logger


def strategy_builder_stg_retrace_ment(instr_days_data, instr_days):
    instr_days_data['stg_retrace_ment'] = None
    for instr_days_record_position, instr_days_record in instr_days.iloc[1:].iterrows():
        instr_day_data = collect_day_specific_data(instr_days_data, instr_days_record)
        current_day = instr_days_record.days
        previous_day = instr_days.iloc[instr_days_record_position - 1].days
        current_candle_dict_info = {'date': current_day}
        current_candle_dict_info = first_candle_type_input_build(instr_days_data, current_day,
                                                                 previous_day, current_candle_dict_info)

        instr_day_orders = pd.DataFrame()
        for row_number, row_record in instr_day_data.iloc[1:].iterrows():
            current_record_direction = row_record.super_trend_direction_7_3
            previous_record_direction = row_record.super_trend_direction_7_3
            current_record_other_direction = row_record.super_trend_direction_7_1
            current_time = row_record.date.time()
            entry_time = datetime.strptime('10:15:00', '%H:%M:%S').time()
            exit_time = datetime.strptime('15:24:00', '%H:%M:%S').time()

            if instr_day_orders.shape[0] == 0:
                retrace_ment_value = ((row_record.super_trend_7_3 - row_record.open) / row_record.super_trend_7_3)
                retrace_ment_per = abs(round(retrace_ment_value * 100, 2))
                if current_time >= entry_time:
                    if (current_record_direction == current_record_other_direction) & (retrace_ment_per < 0.70):
                        first_candle_close = current_candle_dict_info['cur_close']
                        if (current_record_direction == 'up') & (row_record.close > first_candle_close):
                            instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'up_entry'
                            instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number],
                                                                       ignore_index=True)
                        elif (current_record_direction == 'down') & (row_record.close < first_candle_close):
                            instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'down_entry'
                            instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number],
                                                                       ignore_index=True)

            elif (instr_day_orders.shape[0] == 1) & (current_time >= exit_time):
                instr_positional_orders_ = instr_day_orders.iloc[0]
                instr_days_condition = instr_days_data[instr_days_data.date_on_str == row_record.date_on_str]
                exit_condition = exit_order_condition(instr_days_condition)
                if exit_condition:
                    if 'up' in str(instr_positional_orders_.stg_retrace_ment):
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'up_exit'
                        instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number], ignore_index=True)
                    elif 'down' in str(instr_positional_orders_.stg_retrace_ment):
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'down_exit'
                        instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number], ignore_index=True)

            elif (instr_day_orders.shape[0] == 1) & (current_time < exit_time):
                instr_positional_orders_ = instr_day_orders.iloc[0]
                if 'up' in str(instr_positional_orders_.stg_retrace_ment):
                    if current_record_direction != previous_record_direction:
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'up_exit'
                        instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number], ignore_index=True)
                    else:
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = instr_days_data.loc[
                            row_number - 1, 'stg_retrace_ment']

                elif 'down' in str(instr_positional_orders_.stg_retrace_ment):
                    if current_record_direction != previous_record_direction:
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = 'down_exit'
                        instr_day_orders = instr_day_orders.append(instr_days_data.loc[row_number], ignore_index=True)
                    else:
                        instr_days_data.loc[row_number, 'stg_retrace_ment'] = instr_days_data.loc[
                            row_number - 1, 'stg_retrace_ment']

            # Enable -> same -> Double Order
            if instr_day_orders.shape[0] == 2:
                instr_day_orders = pd.DataFrame()
                cus_logger.info("strategy_builder_stg_retrace_ment execution    ---    ended")

    return instr_days_data


def exit_order_condition(instr_days_condition):
    orders = pd.DataFrame()
    instr_days_condition = instr_days_condition.copy().reset_index()
    for row_number_, row_record_ in instr_days_condition.loc[1:].iterrows():
        first_record = instr_days_condition.iloc[row_number_ - 1]
        second_record = instr_days_condition.iloc[row_number_]
        if (first_record.stg_retrace_ment != second_record.stg_retrace_ment) & (
                row_record_.stg_retrace_ment is not None):
            orders = orders.append(second_record)
    if (orders.shape[0] + 2) % 2 != 0:
        return True
    else:
        return False
