from datetime import timedelta
import pandas as pd

from mw_minisoft.trade_lib.tech_indicator.super_trend_builder import super_trend


def build_super_trend_results(df_bk_converted_data, period, multiplier, freq):
    df_bk_converted_data = convert_specific_time_frame(df_bk_converted_data, freq)
    req_columns = ['true_range', 'average_true_range_period_7', 'final_ub', 'final_lb', 'uptrend', 'super_trend_7_3',
                   'super_trend_direction_7_3']
    req_columns_sp = ['date', 'date_on', 'date_on_str', 'open', 'close', 'low', 'high', 'volume', 'super_long_buy_side']
    df_bk_converted_data = df_bk_converted_data.loc[:, ~df_bk_converted_data.columns.str.contains('^Unnamed')]
    df_bk_converted_data = df_bk_converted_data.reset_index()
    df_bk_converted_data['date'] = pd.to_datetime(df_bk_converted_data['date'])
    df_bk_converted_data[req_columns] = None
    default_time_frame = super_trend(df_bk_converted_data, period, multiplier)
    default_time_frame = update_super_long_buy_side_direction(default_time_frame)
    default_time_frame['date'] = pd.to_datetime(default_time_frame['date'])
    default_time_frame["date_on"] = default_time_frame["date"].dt.date
    default_time_frame['date_on_str'] = default_time_frame["date"].dt.date.astype(str)
    re_name_columns = {'update_direction': 'super_long_buy_side'}
    default_time_frame = default_time_frame.rename(columns=re_name_columns)
    default_time_frame = default_time_frame[req_columns_sp]
    return default_time_frame


def convert_specific_time_frame(df_bk_converted_data, freq):
    time_frame_agg_fun = {"open": "first", "close": "last", "low": "min", "high": "max", 'volume': 'sum'}
    df_bk_converted_data = df_bk_converted_data.loc[:, ~df_bk_converted_data.columns.str.contains('^Unnamed')]
    df_bk_converted_data['date'] = pd.to_datetime(df_bk_converted_data['date'])
    df_bk_converted_data = df_bk_converted_data.groupby(pd.Grouper(key='date', freq=freq)).agg(
        time_frame_agg_fun).dropna(how='any')
    return df_bk_converted_data


def update_super_long_buy_side_direction(time_frame_data):
    time_frame_data['update_direction'] = None
    time_frame_data['update_price'] = None
    pd.to_numeric(time_frame_data['update_price'])
    compare_row = None
    for index, row in time_frame_data.iloc[1:].iterrows():
        current_dir = time_frame_data.iloc[index]
        previous_dir = time_frame_data.iloc[index - 1]
        if current_dir.super_trend_direction_7_3 != 'nan' and previous_dir.super_trend_direction_7_3 != 'nan':
            if current_dir.super_trend_direction_7_3 != previous_dir.super_trend_direction_7_3:
                if current_dir.super_trend_direction_7_3 == 'up' and (compare_row is None):
                    compare_row = row
                elif current_dir.super_trend_direction_7_3 == 'up' and (compare_row is not None) and (
                        compare_row.update_price is None):
                    compare_row = row

            if (compare_row is not None) and (row.low > compare_row.close):
                time_frame_data.at[index, 'update_direction'] = 'up_entry'
                if compare_row.update_price is None:
                    compare_row.update_price = row.close

            if (compare_row is not None) and (compare_row.update_price is not None) and (
                    row.close > compare_row.update_price + 350):
                time_frame_data.at[index, 'update_direction'] = 'up_exit'
                compare_row = None

            if (compare_row is not None) and (compare_row.update_price is not None) and (
                    row.close < compare_row.low - 350):
                time_frame_data.at[index, 'update_direction'] = 'up_exit'
                compare_row = None

            if (compare_row is not None) and (compare_row.update_price is not None) and (
                    time_frame_data.at[index, 'update_direction'] is None):
                time_frame_data.at[index, 'update_direction'] = time_frame_data.iloc[index - 1].update_direction

    return time_frame_data


def super_long_buy_side_results(instrument_history_data, auto_inputs, instrument_name):
    default_time_frame = build_super_trend_results(instrument_history_data, period=7, multiplier=4, freq='5min')
    default_time_frame_col = ['date', 'super_long_buy_side']
    default_time_frame = default_time_frame[default_time_frame_col]
    default_time_frame['date'] = default_time_frame['date'] + timedelta(minutes=5)
    default_time_frame = default_time_frame.set_index('date')
    return default_time_frame
