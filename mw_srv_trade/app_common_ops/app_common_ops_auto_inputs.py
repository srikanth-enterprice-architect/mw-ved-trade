import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *
from mw_srv_trade.app_common_trade_logger.logger import cus_logger


def update_auto_inputs(env, minutes, super_trend_period, super_trend_multiplier):
    """
    user input data would be updated on auto_input csv file
   """
    cus_logger.info("updating the user input data into the auto_input.csv file")
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    auto_inputs = pd.DataFrame(auto_inputs).astype(str)
    auto_inputs.at[0, 'scheduler_minutes'] = 1 * minutes
    auto_inputs.at[0, 'data_interval'] = str(1 * minutes)
    auto_inputs.at[0, 'super_trend_period'] = super_trend_period
    auto_inputs.at[0, 'super_trend_multiplier'] = super_trend_multiplier
    auto_inputs.at[0, 'env'] = env
    auto_inputs.to_csv(AUTO_INPUTS_FILE, index=False)
