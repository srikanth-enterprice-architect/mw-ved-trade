import numpy as np
import pandas as pd

from mw_minisoft.trade_logger.logger import cus_logger


def super_trend(history_data, period, multiplier):
    cus_logger.info("super_trend execution    ---    started")
    average_true_range(history_data, period)
    atr = 'average_true_range_period_' + str('7')
    st = 'super_trend_' + str('7') + '_' + str('3')
    stx = 'super_trend_direction_' + str('7') + '_' + str('3')

    history_data['basic_ub'] = (history_data['high'] + history_data['low']) / 2 + multiplier * history_data[atr]
    history_data['basic_lb'] = (history_data['high'] + history_data['low']) / 2 - multiplier * history_data[atr]

    history_data['final_ub'] = 0.00
    history_data['final_lb'] = 0.00

    for i in range(period, len(history_data)):
        if history_data['basic_ub'].iat[i] < history_data['final_ub'].iat[i - 1] or history_data['close'].iat[i - 1] > \
                history_data['final_ub'].iat[i - 1]:
            history_data['final_ub'].iat[i] = history_data['basic_ub'].iat[i]
        else:
            history_data['final_ub'].iat[i] = history_data['final_ub'].iat[i - 1]

        if history_data['basic_lb'].iat[i] > history_data['final_lb'].iat[i - 1] or history_data['close'].iat[i - 1] < \
                history_data['final_lb'].iat[i - 1]:
            history_data['final_lb'].iat[i] = history_data['basic_lb'].iat[i]
        else:
            history_data['final_lb'].iat[i] = history_data['final_lb'].iat[i - 1]

    history_data[st] = 0.00
    for i in range(period, len(history_data)):
        history_data[st].iat[i] = history_data['final_ub'].iat[i] if history_data[st].iat[i - 1] == \
                                                                     history_data['final_ub'].iat[i - 1] and \
                                                                     history_data['close'].iat[i] <= \
                                                                     history_data['final_ub'].iat[i] else \
            history_data['final_lb'].iat[i] if history_data[st].iat[i - 1] == history_data['final_ub'].iat[i - 1] and \
                                               history_data['close'].iat[i] > history_data['final_ub'].iat[i] else \
                history_data['final_lb'].iat[i] if history_data[st].iat[i - 1] == history_data['final_lb'].iat[
                    i - 1] and history_data['close'].iat[i] >= history_data['final_lb'].iat[i] else \
                    history_data['final_ub'].iat[i] if history_data[st].iat[i - 1] == history_data['final_lb'].iat[
                        i - 1] and history_data['close'].iat[i] < history_data['final_lb'].iat[i] else 0.00

    history_data[stx] = np.where((history_data[st] > 0.00),
                                 np.where((history_data['close'] < history_data[st]), 'down', 'up'), np.NaN)

    history_data.drop(['basic_ub', 'basic_lb'], inplace=True, axis=1)
    history_data.fillna(0, inplace=True)
    cus_logger.info("super_trend execution    ---    ended")
    return history_data


def average_true_range(history_data, period):
    cus_logger.info(" average_true_range execution    ---    started")
    average_true_range = 'average_true_range_period_' + str('7')
    if 'true_range' in history_data.columns:
        history_data['high-low'] = history_data['high'] - history_data['low']
        history_data['high-yesterday_close'] = abs(history_data['high'] - history_data['close'].shift())
        history_data['low-yesterday_close'] = abs(history_data['low'] - history_data['close'].shift())
        history_data['true_range'] = history_data[['high-low', 'high-yesterday_close', 'low-yesterday_close']].max(
            axis=1)
        history_data.drop(['high-low', 'high-yesterday_close', 'low-yesterday_close'], inplace=True, axis=1)

    exponential_moving_average(history_data, 'true_range', average_true_range, period, alpha=True)
    cus_logger.info("average_true_range execution    ---    ended")
    return history_data


def exponential_moving_average(history_data, base, target, period, alpha=False):
    cus_logger.info(" exponential_moving_average execution    ---    started")
    con = pd.concat([history_data[:period][base].rolling(window=period).mean(), history_data[period:][base]])
    if alpha:
        history_data[target] = con.ewm(alpha=1 / period, adjust=False).mean()
    else:
        history_data[target] = con.ewm(span=period, adjust=False).mean()
    history_data[target].fillna(0, inplace=True)
    cus_logger.info(" exponential_moving_average execution    ---    ended")
    return history_data
