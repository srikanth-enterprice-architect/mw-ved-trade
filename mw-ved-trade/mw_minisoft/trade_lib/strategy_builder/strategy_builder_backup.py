import pandas as pd
from datetime import timedelta

from mw_minisoft.trade_lib.strategy_builder.day_open_strategy import open_inside_close_strategy_1, \
    open_inside_down_close_outside_strategy, open_in_side_up_close_out_side, up_open_out_side_close_out_side_strategy, \
    down_open_outside_down_close_outside_strategy, down_open_outside_close_in_side_strategy
from mw_minisoft.trade_lib.strategy_builder.strategy_builder_common import collect_day_specific_data
from mw_minisoft.trade_lib.strategy_builder.strategy_formulas import first_candle_type_input_build
from mw_minisoft.trade_lib.tech_indicator.super_trend_builder import super_trend
from mw_minisoft.trade_lib.tech_indicator.vwap_indicator import vwap


def convert_specific_time_frame(raw_df_date, required_time_frame, period, multiplier):
    df_bk_converted_data = raw_df_date
    freq = required_time_frame
    time_frame_agg_fun = {"open": "first", "close": "last", "low": "min", "high": "max", 'volume': 'sum'}
    req_columns = ['true_range', 'average_true_range_period_7', 'final_ub', 'final_lb', 'uptrend', 'super_trend_7_3',
                   'super_trend_direction_7_3']
    df_bk_converted_data = df_bk_converted_data.loc[:, ~df_bk_converted_data.columns.str.contains('^Unnamed')]
    df_bk_converted_data['date'] = pd.to_datetime(df_bk_converted_data['date'])
    df_bk_converted_data = df_bk_converted_data.groupby(pd.Grouper(key='date', freq=freq)).agg(
        time_frame_agg_fun).dropna(how='any')
    df_bk_converted_data[req_columns] = None
    return super_trend(df_bk_converted_data, period, multiplier)


def build_super_trend_results(raw_df_date, period, multiplier):
    df_bk_converted_data = raw_df_date
    req_columns = ['true_range', 'average_true_range_period_7', 'final_ub', 'final_lb', 'uptrend', 'super_trend_7_3',
                   'super_trend_direction_7_3']
    req_columns_sp = ['date', 'date_on', 'date_on_str', 'open', 'close', 'low', 'high', 'volume', 'super_trend_7_3',
                      'super_trend_direction_7_3']
    df_bk_converted_data = df_bk_converted_data.loc[:, ~df_bk_converted_data.columns.str.contains('^Unnamed')]
    df_bk_converted_data['date'] = pd.to_datetime(df_bk_converted_data['date'])
    df_bk_converted_data[req_columns] = None
    default_time_frame = super_trend(df_bk_converted_data, 100, 30)
    default_time_frame['date'] = pd.to_datetime(default_time_frame['date'])
    default_time_frame["date_on"] = default_time_frame["date"].dt.date
    default_time_frame['date_on_str'] = default_time_frame["date"].dt.date.astype(str)
    default_time_frame = default_time_frame[req_columns_sp]
    re_name_columns = {'super_trend_7_3': 'sp_7_3_1min', 'super_trend_direction_7_3': 'sp_dir_7_3_1min'}
    default_time_frame = default_time_frame.rename(columns=re_name_columns)
    return default_time_frame


def strategy_data_builder_(instrument_history_data, auto_inputs, instrument_name):
    default_time_frame = build_super_trend_results(instrument_history_data, period=7, multiplier=3)

    df_bk_one_3min_data = convert_specific_time_frame(instrument_history_data, required_time_frame='3min', period=7,
                                                      multiplier=3)
    df_bk_one_3min_data = df_bk_one_3min_data[['super_trend_7_3', 'super_trend_direction_7_3']]
    df_bk_one_3min_data_col = {'super_trend_7_3': 'sp_7_3_3min', 'super_trend_direction_7_3': 'sp_dir_7_3_3min'}
    df_bk_one_3min_data = df_bk_one_3min_data.rename(columns=df_bk_one_3min_data_col)
    df_bk_one_3min_data = df_bk_one_3min_data.reset_index()
    df_bk_one_3min_data['date'] = df_bk_one_3min_data['date'] + timedelta(minutes=3)
    df_bk_one_3min_data = df_bk_one_3min_data.set_index('date')

    df_bk_one_5min_data = convert_specific_time_frame(instrument_history_data, required_time_frame='5min', period=7,
                                                      multiplier=3)
    df_bk_one_5min_data_7_1 = convert_specific_time_frame(instrument_history_data, required_time_frame='5min', period=7,
                                                          multiplier=1)
    df_bk_one_5min_data['super_trend_direction_7_1'] = df_bk_one_5min_data_7_1['super_trend_direction_7_3']

    df_bk_one_5min_data = df_bk_one_5min_data.reset_index()
    df_bk_one_5min_data['date'] = (pd.to_datetime(df_bk_one_5min_data['date'].copy()))
    df_bk_one_5min_data["date_on"] = df_bk_one_5min_data["date"].copy().dt.date
    df_bk_one_5min_data['date_on_str'] = df_bk_one_5min_data["date"].copy().dt.date.astype(str)
    df_bk_one_5min_group_by_days = df_bk_one_5min_data.groupby(['date_on_str'])
    df_bk_one_5min_group_by_days_data = pd.DataFrame({'days': list(df_bk_one_5min_group_by_days.groups.keys())})
    df_bk_one_5min_data = day_open_strategy(df_bk_one_5min_data, df_bk_one_5min_group_by_days_data, instrument_name)

    df_bk_one_5min_data_col = ['date', 'super_trend_7_3', 'super_trend_direction_7_3', 'super_trend_direction_7_1',
                               'day_open_strategy']
    df_bk_one_5min_data = df_bk_one_5min_data[df_bk_one_5min_data_col]
    df_bk_one_5min_data_col_final = {'super_trend_7_3': 'sp_7_3_5min', 'super_trend_direction_7_3': 'sp_dir_7_3_5min',
                                     'super_trend_direction_7_1': 'sp_dir_7_1_5min'}
    df_bk_one_5min_data = df_bk_one_5min_data.rename(columns=df_bk_one_5min_data_col_final)
    df_bk_one_5min_data['date'] = df_bk_one_5min_data['date'] + timedelta(minutes=5)
    df_bk_one_5min_data = df_bk_one_5min_data.set_index('date')

    df_bk_one_10min_data = convert_specific_time_frame(instrument_history_data, required_time_frame='10min', period=7,
                                                       multiplier=3)
    df_bk_one_10min_data = df_bk_one_10min_data[['super_trend_7_3', 'super_trend_direction_7_3']]
    df_bk_one_10min_data_col = {'super_trend_7_3': 'sp_7_3_10min', 'super_trend_direction_7_3': 'sp_dir_7_3_10min'}
    df_bk_one_10min_data = df_bk_one_10min_data.rename(columns=df_bk_one_10min_data_col)
    df_bk_one_10min_data = df_bk_one_10min_data.reset_index()
    df_bk_one_10min_data['date'] = df_bk_one_10min_data['date'] + timedelta(minutes=10)
    df_bk_one_10min_data = df_bk_one_10min_data.set_index('date')

    merged_dataframe = pd.merge_asof(default_time_frame, df_bk_one_3min_data, on="date")
    merged_dataframe_ = pd.merge_asof(merged_dataframe, df_bk_one_5min_data, on="date")
    return pd.merge_asof(merged_dataframe_, df_bk_one_10min_data, on="date")


def strategy_data_builder(instrument_history_data, auto_inputs, instrument_name):
    df_bank_nifty_super_trend_7_3 = super_trend(instrument_history_data.copy(),
                                                auto_inputs['super_trend_period'][0],
                                                auto_inputs['super_trend_multiplier'][0])

    df_bank_nifty_super_trend_7_1 = super_trend(instrument_history_data.copy(), auto_inputs['super_trend_period'][0], 1)
    df_bank_nifty_super_trend_7_3['super_trend_direction_7_1'] = df_bank_nifty_super_trend_7_1[
        'super_trend_direction_7_3']

    df_bank_nifty_super_trend_7_3 = vwap(df_bank_nifty_super_trend_7_3.copy())

    df_bank_nifty_super_trend_7_3['date'] = (pd.to_datetime(df_bank_nifty_super_trend_7_3['date'].copy()))
    df_bank_nifty_super_trend_7_3["date_on"] = df_bank_nifty_super_trend_7_3["date"].copy().dt.date
    df_bank_nifty_super_trend_7_3['date_on_str'] = df_bank_nifty_super_trend_7_3["date"].copy().dt.date.astype(str)

    df_bank_nifty_super_trend_7_3 = df_bank_nifty_super_trend_7_3[['date_on', 'date_on_str', 'date', 'open',
                                                                   'low', 'high', 'close', 'volume',
                                                                   'super_trend_7_3', 'super_trend_direction_7_3',
                                                                   'super_trend_direction_7_1', 'vwap']]

    df_bank_nifty_intraday_data_group_by_days = df_bank_nifty_super_trend_7_3.groupby(['date_on_str'])
    df_bank_nifty_days_data = pd.DataFrame({'days': list(df_bank_nifty_intraday_data_group_by_days.groups.keys())})

    # df_bank_nifty_super_trend_7_3 = strategy_builder_stg_retrace_ment(df_bank_nifty_super_trend_7_3,df_bank_nifty_days_data)
    # df_bank_nifty_super_trend_7_3 = strategy_builder_hill_base_entry(df_bank_nifty_super_trend_7_3,df_bank_nifty_days_data, instrument_name)

    df_bank_nifty_super_trend_7_3 = day_open_strategy(df_bank_nifty_super_trend_7_3, df_bank_nifty_days_data,
                                                      instrument_name)

    return df_bank_nifty_super_trend_7_3


def day_open_strategy(instr_days_data, instr_days, instrument_name):
    instr_days_data['day_open_strategy'] = None

    for instr_days_record_position, instr_days_record in instr_days.iloc[1:].iterrows():
        instr_day_data = collect_day_specific_data(instr_days_data, instr_days_record)
        current_day = instr_days_record.days
        previous_day = instr_days.iloc[instr_days_record_position - 1].days
        current_candle_dict_info = {'date': current_day}
        current_candle_dict_info = first_candle_type_input_build(instr_days_data, current_day, previous_day,
                                                                 current_candle_dict_info)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_CLOSE_IN_SIDE':
            # win_trade_percentage = 73.73 & profit_trade_percentage = -78.83(profit) & profit_loss_sum = -37764.899999999994 & count = 217 & total = 330
            open_inside_close_strategy_1(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_DOWN_CLOSE_OUT_SIDE':
            # win_trade_percentage = 64.0 & profit_trade_percentage = -74.28 (profit) & profit_loss_sum = -6962.349999999991 & count = 50 & total = 51
            # win_trade_percentage = 92.31 & profit_trade_percentage = -98.83( profit) & profit_loss_sum = -5242.549999999988 & count = 13 & total = 51
            open_inside_down_close_outside_strategy(instr_day_data, current_candle_dict_info, instrument_name,
                                                    instr_days_data)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_UP_CLOSE_OUT_SIDE':
            open_in_side_up_close_out_side(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'UP_OPEN_OUT_SIDE_UP_CLOSE_OUT_SIDE':
            # win_trade_percentage = 69.44 & profit_trade_percentage = -77.98 (profit) & profit_loss_sum = -15370.799999999992 & count = 108 & total: 147
            up_open_out_side_close_out_side_strategy(instr_day_data, current_candle_dict_info, instrument_name,
                                                     instr_days_data)

        if current_candle_dict_info['candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE':
            # win_trade_percentage = 68.29 & profit_trade_percentage = -80.98 (profit) & profit_loss_sum = -7548.94999999999 & count = 41 & total = 88
            # win_trade_percentage = 68.18 & profit_trade_percentage = -80.29 (profit) & profit_loss_sum = -12188.149999999976 & count = 66 & total = 88
            down_open_outside_down_close_outside_strategy(instr_day_data, current_candle_dict_info, instrument_name,
                                                          instr_days_data)

        if current_candle_dict_info['candle_position'] == 'DOWN_OPEN_OUT_SIDE_CLOSE_IN_SIDE':
            down_open_outside_close_in_side_strategy(instr_day_data, current_candle_dict_info, instrument_name,
                                                     instr_days_data)

    return instr_days_data
