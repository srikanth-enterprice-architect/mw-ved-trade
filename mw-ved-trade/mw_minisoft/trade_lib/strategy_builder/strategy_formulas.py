import pandas as pd


def generate_first_candle_info(df_bank_nifty_super_trend_7_3, df_bank_nifty_days_data):
    df_current_candle_dict_info = pd.DataFrame()
    for row_number, row_record in df_bank_nifty_days_data.iterrows():
        current_day = row_record.days
        previous_day = df_bank_nifty_days_data.iloc[row_number - 1].days
        current_candle_dict_info = {'date': current_day}
        if row_number == 0:
            previous_day = current_day
        current_candle_dict_info = first_candle_type_input_build(df_bank_nifty_super_trend_7_3, current_day,
                                                                 previous_day, current_candle_dict_info)
        df_current_candle_dict_info = df_current_candle_dict_info.append(current_candle_dict_info, ignore_index=True)

    return df_current_candle_dict_info


def first_candle_type_input_build(df_bank_nifty_intraday_data, current_day, previous_day, current_candle_dict_info):
    df_bank_nifty_data_previous = df_bank_nifty_intraday_data[
        (df_bank_nifty_intraday_data['date_on_str'] == previous_day)]

    df_bank_nifty_data_current = df_bank_nifty_intraday_data[
        (df_bank_nifty_intraday_data['date_on_str'] == current_day)]

    df_bank_nifty_intraday_data_previous = pd.DataFrame([{'pre_open': df_bank_nifty_data_previous.open.iloc[0],
                                                          'pre_high': df_bank_nifty_data_previous.high.max(),
                                                          'pre_low': df_bank_nifty_data_previous.low.min(),
                                                          'pre_close': df_bank_nifty_data_previous.close.iloc[-1]}],
                                                        )

    df_bank_nifty_intraday_data_current = pd.DataFrame([{'cur_open': df_bank_nifty_data_current.iloc[0].open,
                                                         'cur_high': df_bank_nifty_data_current.iloc[0].high,
                                                         'cur_low': df_bank_nifty_data_current.iloc[0].low,
                                                         'cur_close': df_bank_nifty_data_current.iloc[0].close
                                                         }])
    current_candle_dict_info = first_candle_type(df_bank_nifty_intraday_data_current,
                                                 df_bank_nifty_intraday_data_previous, current_candle_dict_info)

    current_candle_dict_info = current_first_candle_info(df_bank_nifty_intraday_data_current, current_candle_dict_info)

    current_candle_dict_info = current_candle_dict_info | (
        df_bank_nifty_intraday_data_current.to_dict(orient='records')[0]) | (
                                   df_bank_nifty_intraday_data_previous.to_dict(orient='records')[0])

    return current_candle_dict_info


def first_candle_type(df_bank_nifty_intraday_data_current, df_bank_nifty_intraday_data_previous, current_day_dict):
    cur_open = df_bank_nifty_intraday_data_current['cur_open']
    pre_low = df_bank_nifty_intraday_data_previous['pre_low']
    pre_high = df_bank_nifty_intraday_data_previous['pre_high']
    cur_close = df_bank_nifty_intraday_data_current['cur_close']

    if (cur_open.values[0] < pre_low.values[0]) and (cur_close.values[0] < pre_low.values[0]):
        current_day_dict['candle_position'] = 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE'

    elif (cur_open.values[0] < pre_low.values[0]) and (cur_close.values[0] > pre_low.values[0]):
        current_day_dict['candle_position'] = 'DOWN_OPEN_OUT_SIDE_CLOSE_IN_SIDE'

    elif (cur_open.values[0] > pre_low.values[0]) and (cur_close.values[0] < pre_low.values[0]):
        current_day_dict['candle_position'] = 'OPEN_IN_SIDE_DOWN_CLOSE_OUT_SIDE'

    elif (cur_open.values[0] < pre_high.values[0]) and (cur_close.values[0] > pre_low.values[0]) and (
            cur_close.values[0] < pre_high.values[0]):
        current_day_dict['candle_position'] = 'OPEN_IN_SIDE_CLOSE_IN_SIDE'

    elif (cur_open.values[0] < pre_high.values[0]) and (cur_close.values[0] > pre_high.values[0]):
        current_day_dict['candle_position'] = 'OPEN_IN_SIDE_UP_CLOSE_OUT_SIDE'

    elif (cur_open.values[0] > pre_high.values[0]) and (cur_close.values[0] > pre_high.values[0]):
        current_day_dict['candle_position'] = 'UP_OPEN_OUT_SIDE_UP_CLOSE_OUT_SIDE'

    elif (cur_open.values[0] > pre_high.values[0]) and (cur_close.values[0] < pre_high.values[0]):
        current_day_dict['candle_position'] = 'UP_OPEN_OUT_SIDE_CLOSE_IN_SIDE'

    elif (cur_open.values[0] < pre_low.values[0]) and (cur_close.values[0] > pre_high.values[0]):
        current_day_dict['candle_position'] = 'DOWN_OPEN_OUT_SIDE_UP_CLOSE_OUT_SIDE'

    elif (cur_open.values[0] > pre_high.values[0]) and (cur_close.values[0] < pre_low.values[0]):
        current_day_dict['candle_position'] = 'UP_OPEN_OUT_SIDE_DOWN_CLOSE_OUT_SIDE'

    else:
        current_day_dict['candle_position'] = 'NO_CLEAR_DIRECTION'

    return current_day_dict


def current_first_candle_info(df_bank_nifty_intraday_data_current, current_day_dict):
    cur_open = df_bank_nifty_intraday_data_current['cur_open'].values[0]
    cur_close = df_bank_nifty_intraday_data_current['cur_close'].values[0]
    cur_high = df_bank_nifty_intraday_data_current['cur_high'].values[0]
    cur_low = df_bank_nifty_intraday_data_current['cur_low'].values[0]

    if cur_open > cur_close:
        current_day_dict['candle_type'] = 'red candle'
        current_day_dict['candle_upper_wick'] = abs(round((((cur_open - cur_high) / cur_open) * 100), 2))
        current_day_dict['candle_lower_wick'] = abs(round((((cur_close - cur_low) / cur_close) * 100), 2))
    else:
        current_day_dict['candle_type'] = 'green candle'
        current_day_dict['candle_upper_wick'] = abs(round((((cur_close - cur_high) / cur_close) * 100), 2))
        current_day_dict['candle_lower_wick'] = abs(round((((cur_open - cur_low) / cur_open) * 100), 2))

    current_day_dict['candle_width'] = abs(round((((cur_high - cur_low) / cur_high) * 100), 2))
    current_day_dict['candle_body'] = abs(round((((cur_open - cur_close) / cur_open) * 100), 2))

    return current_day_dict
