from datetime import date

import pandas as pd

from mw_srv_trade.app_common_ops.app_common_trade_constants import *
from mw_srv_trade.app_common_trade_lib.session_builder.retrive_request_token import create_user_session
from mw_srv_trade.app_common_trade_logger.logger import cus_logger


def write_user_info(user_id, auth_code, refresh_token, access_token, auth_token_date):
    """
    generated user session tokens will be stored
    """
    cus_logger.info("storing user token information in the file sys")
    user_records = pd.read_csv(USER_INPUTS_FILE)
    user_records_data = pd.DataFrame(user_records).astype(str)
    for user_record_position, user_info_record in user_records_data.iterrows():
        if user_info_record['user_id'] == user_id:
            user_records_data.at[user_record_position, 'auth_code'] = auth_code
            user_records_data.at[user_record_position, 'day'] = date.today().day
            user_records_data.at[user_record_position, 'refresh_token'] = refresh_token
            user_records_data.at[user_record_position, 'access_token'] = access_token
            user_records_data.at[user_record_position, 'auth_token_date'] = auth_token_date
    user_records_data['login_pin'].astype(str)
    user_records_data.to_csv(USER_INPUTS_FILE, index=False)
    return user_records_data


def download_each_user_tokens():
    """
    This code will be used to obtain the accessToken from the source system, which will then be used to access the
    order API and other services.
    """
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info = user_info[user_info.day != date.today().day]
    for user_record_position, user_record in user_info.iterrows():
        cus_logger.info("user(%s) session token generation started", user_record.user_id)
        user_kite_session, user_record = create_user_session(user_record, FIREFOX_DRIVER_PATH)
        write_user_info(user_record.user_id, user_record.auth_code, user_record.refresh_token, user_record.access_token,
                        user_record.auth_token_date)
    cus_logger.info("session token generation completed ")
