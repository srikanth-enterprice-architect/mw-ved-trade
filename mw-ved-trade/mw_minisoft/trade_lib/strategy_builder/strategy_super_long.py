from datetime import timedelta
import pandas as pd

from mw_minisoft.trade_lib.tech_indicator.super_trend_builder import super_trend


def strategy_super_long_data_builder(instrument_history_data, auto_inputs, instrument_name):
    df_bk_one_5min_data = convert_specific_time_frame(instrument_history_data, required_time_frame='10min', period=10,
                                                      multiplier=1)
    df_bk_one_5min_data = df_bk_one_5min_data.reset_index()
    df_bk_one_5min_data['date'] = (pd.to_datetime(df_bk_one_5min_data['date'].copy()))
    df_bk_one_5min_data["date_on"] = df_bk_one_5min_data["date"].copy().dt.date
    df_bk_one_5min_data['date_on_str'] = df_bk_one_5min_data["date"].copy().dt.date.astype(str)
    df_bk_one_5min_group_by_days = df_bk_one_5min_data.groupby(['date_on_str'])
    df_bk_one_5min_group_by_days_data = pd.DataFrame({'days': list(df_bk_one_5min_group_by_days.groups.keys())})
    df_bk_one_5min_data = strategy_super_long(df_bk_one_5min_data, df_bk_one_5min_group_by_days_data, instrument_name)
    df_bk_one_5min_data_col = ['date', 'strategy_super_long']
    df_bk_one_5min_data = df_bk_one_5min_data[df_bk_one_5min_data_col]
    df_bk_one_5min_data['date'] = df_bk_one_5min_data['date'] + timedelta(minutes=10)
    df_bk_one_5min_data = df_bk_one_5min_data.set_index('date')
    return df_bk_one_5min_data


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


def strategy_super_long(data, instr_days, instrument_name):
    data['strategy_super_long'] = None
    compare_row = None
    for index, row in data.iterrows():
        if not pd.isna(data.iloc[index - 1].super_trend_direction_7_3) and not pd.isna(
                data.iloc[index].super_trend_direction_7_3):
            if data.iloc[index].super_trend_direction_7_3 != data.iloc[index - 1].super_trend_direction_7_3:
                data.at[index, 'strategy_super_long'] = data.iloc[index - 1].strategy_super_long
                compare_row = row
            elif compare_row is not None:
                if compare_row.super_trend_direction_7_3 == 'up':
                    if row.final_lb > compare_row.close:
                        data.at[index, 'strategy_super_long'] = row.super_trend_direction_7_3
                    else:
                        data.at[index, 'strategy_super_long'] = data.iloc[index - 1].strategy_super_long
                elif compare_row.super_trend_direction_7_3 == 'down':
                    if row.final_ub < compare_row.close:
                        data.at[index, 'strategy_super_long'] = row.super_trend_direction_7_3
                    else:
                        data.at[index, 'strategy_super_long'] = data.iloc[index - 1].strategy_super_long

    return data
