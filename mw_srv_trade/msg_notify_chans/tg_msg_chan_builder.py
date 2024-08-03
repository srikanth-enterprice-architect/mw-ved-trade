from os import path
import requests

from mw_srv_trade.msg_notify_chans.org_msg_prep import exit_order_preparation, day_order_dict_pre
from mw_srv_trade.persist_ops.account_management import *


def send_to_telegram(ind_record, inst_last_record_value, sp_user_session, inst_order_record, strategy_name):
    client_capital_book = pd.read_csv(CLIENT_CAPITAL)
    day_instrument_orders_file = DAY_INSTRUMENT_ORDERS
    if 'entry' in inst_last_record_value and path.exists(day_instrument_orders_file):
        day_instrument_orders = pd.read_csv(DAY_INSTRUMENT_ORDERS)
        day_order_dict = day_order_dict_pre(ind_record, inst_order_record, client_capital_book,
                                            day_instrument_orders, sp_user_session, strategy_name)
        day_instrument_orders = day_instrument_orders.append(day_order_dict[0], ignore_index=True)
        client_capital_book.at[0, 'client_current_capital'] = day_order_dict[0]['client current capital']
        sent_telegram_message(day_instrument_orders.iloc[-1])
        day_instrument_orders.to_csv(day_instrument_orders_file, index=False)
        client_capital_book.to_csv(CLIENT_CAPITAL, index=False)

    if 'exit' in inst_last_record_value and path.exists(day_instrument_orders_file):
        day_instrument_orders, day_inst_orders_index = exit_order_preparation(client_capital_book, ind_record,
                                                                              inst_order_record, sp_user_session,
                                                                              strategy_name)
        client_capital_book.at[0, 'client_current_capital'] = day_instrument_orders.iloc[day_inst_orders_index][
            'client current capital']
        client_capital_book.to_csv(CLIENT_CAPITAL, index=False)
        day_instrument_orders.to_csv(day_instrument_orders_file, index=False)
        sent_telegram_message(day_instrument_orders.iloc[day_inst_orders_index])


def sent_telegram_message(day_instrument_orders):
    api_token = '5595208984:AAFhBhFLDrR52eNNgo7fvu-gV-FsYaN9X5k'
    api_telegram_url = f'https://api.telegram.org/bot{api_token}/sendMessage'
    chat_id = '@algo_day_open'
    telegram_message_txt = str(day_instrument_orders.to_string())
    telegram_message = {'chat_id': chat_id, 'text': telegram_message_txt, 'parse_mode': 'html'}
    response = requests.post(api_telegram_url, json=telegram_message)
    cus_logger.info(response.text)


