from datetime import datetime

import numpy as np
from mw_srv_trade.app_common_trade_lib.strategy_builder.strategy_builder_common import exit_entry_time
from mw_srv_trade.app_common_trade_lib.tech_indicator.app_com_tech_indi import *
from mw_srv_trade.app_common_trade_lib.tech_indicator.super_trend_builder import super_trend

raw_df_date = pd.read_csv('BSE_SENSEX-INDEX_5.csv')


def inst_opt_ord_str(raw_df_date):
    macd_cal_data = calculate_macd(raw_df_date)
    rsi_cal_data = calculate_rsi(macd_cal_data, period=14)
    super_trend_cal_data = calculate_super_trend(rsi_cal_data, 7, 1)
    super_trend_cal_data['inst_opt_deci_mak'] = np.nan
    inst_days = super_trend_cal_data.date_on_str.unique()
    for inst_day in inst_days:
        inst_day_data = super_trend_cal_data[super_trend_cal_data.date_on_str == inst_day]
        inst_day_skip = inst_day_data[inst_day_data.Signal == 0]
        inst_flt_indi_day_data = inst_day_data
        new_orders = pd.DataFrame()
        new_updated_orders = pd.DataFrame()
        if inst_day_skip.shape[0] == 0:
            exit_time_ = exit_entry_time("NSE:instrument_name")
            for inst_flt_indi_day_rec_index, inst_flt_indi_day_rec in inst_flt_indi_day_data.iterrows():
                current_time = datetime.strptime(inst_flt_indi_day_rec.date.split(' ')[1].split('+')[0],
                                                 '%H:%M:%S').time()
                exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
                if inst_flt_indi_day_rec['In Uptrend'] and new_orders.size == 0:
                    row_number_update_ = super_trend_cal_data.loc[[inst_flt_indi_day_rec_index]]
                    new_orders = pd.concat([new_orders, row_number_update_], ignore_index=True)
                elif new_orders.shape[0] > 0:
                    if inst_flt_indi_day_rec.close > new_orders.iloc[-1].close:
                        if new_updated_orders.shape[0] == 0:
                            row_number_update_ = super_trend_cal_data.loc[[inst_flt_indi_day_rec_index]]
                            new_updated_orders = pd.concat([new_updated_orders, row_number_update_], ignore_index=True)
                        super_trend_cal_data.loc[inst_flt_indi_day_rec_index, 'inst_opt_deci_mak'] = 'up_entry'
                    elif inst_flt_indi_day_rec.close < new_orders.iloc[-1]['Lower Band'] and new_updated_orders.shape[
                        0] > 0:
                        new_orders = pd.DataFrame()
                        new_updated_orders = pd.DataFrame()
                    elif new_updated_orders.shape[0] > 0:
                        super_trend_cal_data.loc[inst_flt_indi_day_rec_index, 'inst_opt_deci_mak'] = 'up_entry'
                if current_time >= exit_time:
                    super_trend_cal_data.loc[inst_flt_indi_day_rec_index, 'inst_opt_deci_mak'] = np.nan


inst_opt_ord_str()
