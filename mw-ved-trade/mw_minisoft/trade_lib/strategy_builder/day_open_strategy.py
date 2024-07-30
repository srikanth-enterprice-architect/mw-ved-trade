from datetime import datetime
import pandas as pd

from mw_minisoft.trade_lib.strategy_builder.strategy_builder_common import update_record_after_exit_time_strategy, \
    exit_entry_time, update_record_before_exit_time_strategy


def open_inside_type_open_close_strategy(current_candle_dict_info):
    open_inside_candle_type = 'None'
    candle_type_red = current_candle_dict_info['candle_type'] == 'red candle'
    candle_type_green = current_candle_dict_info['candle_type'] == 'green candle'
    day_cur_open_pre_close_above = current_candle_dict_info['cur_open'] > current_candle_dict_info['pre_close']
    day_cur_close_pre_close_above = current_candle_dict_info['cur_close'] > current_candle_dict_info['pre_close']

    day_cur_open_pre_close_below = current_candle_dict_info['cur_open'] < current_candle_dict_info['pre_close']
    day_cur_close_pre_close_below = current_candle_dict_info['cur_close'] < current_candle_dict_info['pre_close']

    if candle_type_red and day_cur_open_pre_close_above and day_cur_close_pre_close_above:
        open_inside_candle_type = "pre_open_close_above_close_red"

    elif candle_type_green and day_cur_open_pre_close_above and day_cur_close_pre_close_above:
        open_inside_candle_type = "pre_open_close_above_close_green"

    elif candle_type_green and day_cur_open_pre_close_below and day_cur_close_pre_close_below:
        open_inside_candle_type = "pre_open_close_below_close_green"

    elif candle_type_red and day_cur_open_pre_close_below and day_cur_close_pre_close_below:
        open_inside_candle_type = "pre_open_close_below_close_red"

    elif day_cur_open_pre_close_below and day_cur_close_pre_close_above:
        open_inside_candle_type = "pre_open_below_close_above"

    elif day_cur_open_pre_close_above and day_cur_close_pre_close_below:
        open_inside_candle_type = "pre_open_above_close_below"

    return open_inside_candle_type


def open_inside_close_strategy_1(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    open_inside_candle_type = open_inside_type_open_close_strategy(current_candle_dict_info)
    if open_inside_candle_type == "pre_open_close_above_close_red":
        pre_close_open_close_above_red(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    if open_inside_candle_type == "pre_open_close_above_close_green":
        pre_close_open_close_above_green(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    if open_inside_candle_type == "pre_open_close_below_close_green":
        pre_open_close_below_close_green(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    if open_inside_candle_type == "pre_open_close_below_close_red":
        pre_open_close_below_close_red(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    if open_inside_candle_type == "pre_open_below_close_above":
        pre_open_below_close_above(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    if open_inside_candle_type == "pre_open_above_close_below":
        pre_open_above_close_below(current_day_data, current_candle_dict_info, instrument_name, instr_days_data)


def pre_open_above_close_below(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()

        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        pre_close = current_candle_dict_info['pre_close']
        cur_low = current_candle_dict_info['cur_low']
        pre_low = current_candle_dict_info['pre_low']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        third_entry_condition = cur_open > pre_close
        fourth_entry_condition = cur_close < pre_close

        all_conditions = first_entry_condition and third_entry_condition and fourth_entry_condition

        if (current_time < exit_time):

            if all_conditions and (cur_low - pre_low) < 150:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def pre_open_below_close_above(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()

        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        pre_close = current_candle_dict_info['pre_close']
        cur_low = current_candle_dict_info['cur_low']
        pre_low = current_candle_dict_info['pre_low']
        pre_open = current_candle_dict_info['pre_open']
        pre_high = current_candle_dict_info['pre_high']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        third_entry_condition = cur_open < pre_close
        fourth_entry_condition = cur_close > pre_close

        all_conditions = first_entry_condition and third_entry_condition and fourth_entry_condition

        if (current_time < exit_time):

            if all_conditions and row_record.low < cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and pre_open > pre_close and row_record.open > pre_high:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def pre_open_close_below_close_red(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()

        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        cur_high = current_candle_dict_info['cur_high']
        pre_close = current_candle_dict_info['pre_close']
        pre_high = current_candle_dict_info['pre_high']
        cur_low = current_candle_dict_info['cur_low']
        pre_low = current_candle_dict_info['pre_low']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        second_entry_condition = current_candle_dict_info['candle_type'] == 'green candle'
        third_entry_condition = cur_open < pre_close
        fourth_entry_condition = cur_close < pre_close

        fifth_entry_condition = row_record.low > pre_low

        all_conditions = first_entry_condition and second_entry_condition and third_entry_condition and fourth_entry_condition and fifth_entry_condition

        if (current_time < exit_time):

            if all_conditions and (cur_low - pre_low) < 150:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and (cur_low - pre_low) > 150:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def pre_open_close_below_close_green(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()

        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        cur_high = current_candle_dict_info['cur_high']
        pre_close = current_candle_dict_info['pre_close']
        pre_high = current_candle_dict_info['pre_high']
        cur_low = current_candle_dict_info['cur_low']
        pre_open = current_candle_dict_info['pre_open']
        pre_low = current_candle_dict_info['pre_low']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        second_entry_condition = current_candle_dict_info['candle_type'] == 'green candle'
        third_entry_condition = cur_open < pre_close
        fourth_entry_condition = cur_close < pre_close

        fifth_entry_condition = row_record.close < cur_low
        sixth_entry_condition = row_record.low > pre_high

        all_conditions = first_entry_condition and second_entry_condition and third_entry_condition and fourth_entry_condition

        if (current_time < exit_time):

            if all_conditions and pre_open > pre_close and pre_close - pre_low < 200:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and row_record.close < cur_open and pre_close - pre_low > 300:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and row_record.low > pre_high:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def pre_close_open_close_above_green(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()

        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        cur_high = current_candle_dict_info['cur_high']
        pre_close = current_candle_dict_info['pre_close']
        pre_high = current_candle_dict_info['pre_high']
        pre_open = current_candle_dict_info['pre_open']
        pre_low = current_candle_dict_info['pre_low']
        cur_low = current_candle_dict_info['cur_low']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        second_entry_condition = current_candle_dict_info['candle_type'] == 'green candle'
        third_entry_condition = cur_open > pre_close
        fourth_entry_condition = cur_close > pre_close

        fifth_entry_condition = (cur_high - pre_high) < 0
        fifth_entry_condition_1 = (cur_high - pre_high) > 0

        sixth_entry_condition = abs(cur_high - pre_high) > 150

        all_conditions = first_entry_condition and second_entry_condition and third_entry_condition and fourth_entry_condition

        all_conditions_grat_pco = current_candle_dict_info['pre_close'] > current_candle_dict_info['pre_open']
        all_conditions_less_pco = current_candle_dict_info['pre_close'] < current_candle_dict_info['pre_open']

        if (current_time < exit_time):
            if all_conditions and all_conditions_less_pco and cur_high < pre_open and row_record.low < pre_close and int(
                    pre_low - row_record.close) < 50:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and row_record.open < cur_low and all_conditions_grat_pco:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and all_conditions_grat_pco and (cur_high - pre_high) > 0:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and row_record.close - pre_high > 400:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def pre_close_open_close_above_red(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_open = current_candle_dict_info['cur_open']
        cur_close = current_candle_dict_info['cur_close']
        cur_high = current_candle_dict_info['cur_high']
        pre_close = current_candle_dict_info['pre_close']
        pre_high = current_candle_dict_info['pre_high']
        cur_low = current_candle_dict_info['cur_low']
        pre_open = current_candle_dict_info['pre_open']
        pre_low = current_candle_dict_info['pre_low']

        first_entry_condition = instr_positional_orders.shape[0] == 0
        second_entry_condition = current_candle_dict_info['candle_type'] == 'red candle'
        third_entry_condition = cur_open > pre_close
        fourth_entry_condition = cur_close > pre_close
        fifth_entry_condition = row_record.close > cur_high
        fifth_entry_condition_1 = row_record.low > pre_high
        all_conditions = first_entry_condition and second_entry_condition and third_entry_condition and fourth_entry_condition

        if (current_time < exit_time):

            if all_conditions and cur_low > pre_close and fifth_entry_condition and pre_close < pre_open and (
                    row_record.close - pre_high) < -150:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and pre_close > pre_open and (row_record.close - pre_low) < -200:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif all_conditions and pre_close < pre_open and (row_record.close - pre_low) < -400:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def open_inside_close_strategy(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    primary_entry_condition_ = False
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        pre_open = current_candle_dict_info['pre_open']
        pre_high = current_candle_dict_info['pre_high']
        pre_low = current_candle_dict_info['pre_low']
        pre_close = current_candle_dict_info['pre_close']
        cur_close = current_candle_dict_info['cur_open']
        cur_open = current_candle_dict_info['cur_open']
        first_candle_type_ = current_candle_dict_info['candle_type']

        high_diff_per = abs(round(((pre_high - cur_close) / pre_high) * 100, 2))
        low_diff_per = abs(round(((pre_low - cur_close) / pre_low) * 100, 2))

        green_candle = (instr_positional_orders.shape[0] == 0)
        red_candle = (instr_positional_orders.shape[0] == 0)

        if current_day_data.index[1] == row_number:
            primary_entry_condition_ = row_record.close < pre_close

        if (current_time < exit_time):

            if green_candle and pre_open > pre_close > cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif red_candle and pre_open > pre_close and row_record.low > pre_high:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif red_candle and pre_close > pre_open > cur_open and row_record.low < pre_low:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def open_inside_down_close_outside_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                            instr_days_data):
    profit_loss_day_df_new = pd.DataFrame()
    second_entry_condition_buy = False
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    condition_1 = False
    condition_2 = False

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_low = current_candle_dict_info['cur_low']
        first_candle_type_ = current_candle_dict_info['candle_type']

        primary_entry_condition_sell = (instr_positional_orders.shape[0] == 0)
        pre_high = current_candle_dict_info['pre_high']
        pre_open = current_candle_dict_info['pre_open']
        pre_low = current_candle_dict_info['pre_low']
        cur_open = current_candle_dict_info['cur_open']
        o_l = pre_open - pre_low
        h_l = pre_high - pre_low

        if not condition_1 and row_record.low > current_candle_dict_info['pre_close']:
            condition_1 = True

        if not condition_2 and row_record.close < current_candle_dict_info['cur_low']:
            condition_2 = True

        if (current_time < exit_time):

            if primary_entry_condition_sell and row_record.close > pre_high:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and condition_1 and not condition_2 and row_record.close < cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and o_l < 100 and row_record.high > cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and abs(o_l - h_l) < 10 and row_record.high > cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def open_in_side_up_close_out_side(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    profit_loss_day_df_new = pd.DataFrame()
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_low = current_candle_dict_info['cur_low']
        first_candle_type_ = current_candle_dict_info['candle_type']
        cur_high = current_candle_dict_info['cur_high']

        previous_record = current_day_data.loc[row_number - 1].super_trend_direction_7_1
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        primary_entry_condition_sell = (instr_positional_orders.shape[0] == 0) and (
                first_candle_type_ == 'green candle')

        if previous_record != current_record:
            profit_loss_df = {'date_on_str': row_record.date_on_str, 'instrument': 'bank_nifty',
                              'traded_date_time': row_record.date,
                              'direction': current_record, 'price': row_record.close}
            profit_loss_day_df_new = profit_loss_day_df_new.append(profit_loss_df, ignore_index=True)

        shape_ = profit_loss_day_df_new.shape[0]

        if (current_time < exit_time):

            if primary_entry_condition_sell and row_record.close < cur_low and shape_ > 0 and current_record == 'up':
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and row_record.close > cur_high and shape_ > 0 and current_record == 'down':
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (instr_positional_orders.shape[0] == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (instr_positional_orders.shape[0] == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def up_open_out_side_close_out_side_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                             instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    # win_trade_percentage = 69.44 & profit_trade_percentage = -77.98 (profit) & profit_loss_sum = -15370.799999999992 & count = 108
    condition_12 = False
    for row_number, row_record in current_day_data.iloc[2:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_open = current_candle_dict_info['cur_open']
        cur_low = current_candle_dict_info['cur_low']
        pre_high = current_candle_dict_info['pre_high']
        cur_high = current_candle_dict_info['cur_high']
        cur_close = current_candle_dict_info['cur_close']

        current_record = current_day_data.loc[row_number].super_trend_direction_7_1
        shape_ = instr_positional_orders.shape[0]

        condition_01 = (shape_ == 0) and cur_low > pre_high and current_candle_dict_info[
            'candle_type'] == 'green candle' and row_record.open < cur_close
        condition_02 = (shape_ == 0) and current_candle_dict_info['candle_type'] == 'red candle'

        if current_candle_dict_info['candle_type'] == 'red candle' and not condition_12 and row_record.open < pre_high:
            condition_12 = True

        if (current_time < exit_time):
            if condition_01:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif condition_02 and row_record.high > cur_high and current_record == 'down':
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif condition_02 and condition_12 and row_record.high > cur_close and current_record == 'up':
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (shape_ == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders, row_number,
                                                        row_record,
                                                        instr_days_data)

        if (shape_ == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def down_open_outside_down_close_outside_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                                  instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    second_entry_condition_buy = False
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        shape_ = instr_positional_orders.shape[0]
        cur_low = current_candle_dict_info['cur_low']
        cur_open = current_candle_dict_info['cur_open']
        cur_high = current_candle_dict_info['cur_high']
        cur_close = current_candle_dict_info['cur_close']
        first_candle_type_ = current_candle_dict_info['candle_type']

        primary_entry_condition_buy = (shape_ == 0) and (first_candle_type_ == 'red candle')
        primary_entry_condition_sell = (shape_ == 0) and (first_candle_type_ == 'green candle')

        if current_day_data.index[1] == row_number:
            second_entry_condition_buy = row_record.low > cur_low
            second_entry_condition_sell = row_record.high > cur_high
            second_entry_condition_sell_ = row_record.high < cur_high

        if (current_time < exit_time):

            if primary_entry_condition_buy and second_entry_condition_buy:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_buy and row_record.close > cur_open and current_record == 'up':
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and second_entry_condition_sell and row_record.close < cur_open:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif primary_entry_condition_sell and second_entry_condition_sell_ and row_record.open < cur_close:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (shape_ == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (shape_ == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def down_open_outside_close_in_side_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                             instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        shape_ = instr_positional_orders.shape[0]
        pre_close = current_candle_dict_info['pre_close']
        first_candle_type_ = current_candle_dict_info['candle_type']

        diff_super_trend = abs(round(((row_record.close - pre_close) / row_record.close) * 100, 2))
        primary_entry_condition_sell = (shape_ == 0) and (first_candle_type_ == 'green candle')

        if (current_time < exit_time):

            if primary_entry_condition_sell and diff_super_trend < 0.30:
                instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
                row_number_update_ = instr_days_data.loc[row_number]
                instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

            elif (shape_ == 1) and (current_time < exit_time):
                update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                        row_number, row_record, instr_days_data)

        if (shape_ == 1) and (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)
