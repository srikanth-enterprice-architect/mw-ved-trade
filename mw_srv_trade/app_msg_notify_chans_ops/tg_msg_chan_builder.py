import requests

from mw_srv_trade.app_common_trade_logger.logger import cus_logger
from mw_srv_trade.app_msg_notify_chans_ops.tel_message_templates import build_tel_notify_message


def sent_telegram_message(day_instrument_orders):
    api_token = '5595208984:AAFhBhFLDrR52eNNgo7fvu-gV-FsYaN9X5k'
    api_telegram_url = f'https://api.telegram.org/bot{api_token}/sendMessage'
    chat_id = '@algo_based_trading'
    telegram_message_txt = (build_tel_notify_message(day_instrument_orders).iloc[-1].to_string())
    telegram_message = {'chat_id': chat_id, 'text': telegram_message_txt, 'parse_mode': 'html'}
    response = requests.post(api_telegram_url, json=telegram_message)
    cus_logger.info(response.text)


def sent_telegram_message_backup(day_instrument_orders):
    api_token = '5595208984:AAFhBhFLDrR52eNNgo7fvu-gV-FsYaN9X5k'
    api_telegram_url = f'https://api.telegram.org/bot{api_token}/sendMessage'
    chat_id = '@algo_day_open'
    telegram_message_txt = f"<b>Day Instrument Orders:</b>\n{day_instrument_orders.to_string()}"  # Convert dataframe or text
    # telegram_message_txt = str(day_instrument_orders.to_string())
    telegram_message = {'chat_id': chat_id, 'text': telegram_message_txt, 'parse_mode': 'html'}
    response = requests.post(api_telegram_url, json=telegram_message)
    cus_logger.info(response.text)
