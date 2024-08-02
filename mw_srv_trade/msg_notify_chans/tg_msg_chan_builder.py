from os import path
import requests

from mw_srv_trade.msg_notify_chans.org_msg_prep import exit_order_preparation, day_order_dict_pre
from mw_srv_trade.persist_ops.account_management import *


def send_to_telegram(ind_record, inst_last_record_value, sp_user_session, inst_order_record, strategy_name):
    client_capital_book = pd.read_csv(CLIENT_CAPITAL)
    day_instrument_orders_file = DAY_INSTRUMENT_ORDERS
    if 'entry' in inst_last_record_value and path.exists(day_instrument_orders_file):
        day_instrument_orders = pd.read_csv(DAY_INSTRUMENT_ORDERS)
        day_order_dict = day_order_dict_pre(ind_record, inst_order_record, client_capital_book, day_instrument_orders,
                                            sp_user_session, strategy_name)
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
    #response = requests.post(api_telegram_url, json=telegram_message)
    # if (day_instrument_orders.strategy_name == 'day_open_strategy') and ('BANK' in day_instrument_orders['instrument name']):
    # sent_other_telegram(api_telegram_url, day_instrument_orders)
    #cus_logger.info(response.text)


def sent_postman_req(day_instrument_orders):
    url = 'http://13.127.40.204/webhook'
    if day_instrument_orders['instrument exit type'] == '':
        entry_type = "sell"
    else:
        entry_type = "buy"
    postman_json = {
        "instrument_name": "NSE:BANKNIFTY", "start_name": "day_open_strategy", "entry_type": entry_type,
        "expiry_day": "N"
    }
    postman_json_request = requests.post(url, json=postman_json)


def sent_other_telegram(api_telegram_url, day_instrument_orders):
    chat_id = '@algo_trading_stg_00'
    automation = [{'inst date': date.today(),
                   'inst name': day_instrument_orders['instrument name'],
                   'inst price': day_instrument_orders['instrument price'],
                   'inst entry type': day_instrument_orders['instrument entry type'],
                   'inst entry time': day_instrument_orders['instrument entry time'],
                   'inst entry price': day_instrument_orders['instrument entry price'],
                   'inst entry qty': day_instrument_orders['instrument entry qty'],
                   'inst exit type': day_instrument_orders['instrument exit type'],
                   'inst exit qty': day_instrument_orders['instrument exit qty'],
                   'inst exit time': day_instrument_orders['instrument exit time'],
                   'inst exit price': day_instrument_orders['instrument exit price'],
                   'inst profit': day_instrument_orders['instrument profit'],
                   'inst profit or loss': day_instrument_orders['instrument profit or loss']}]
    telegram_message_txt = str((pd.DataFrame(automation).iloc[-1]))
    telegram_message = {'chat_id': chat_id, 'text': telegram_message_txt, 'parse_mode': 'html'}
    requests.post(api_telegram_url, json=telegram_message)


def sent_other_telegram_main(api_telegram_url, day_instrument_orders):
    chat_id = '@automation_algo_01'
    automation = [{'inst date': date.today(),
                   'inst name': day_instrument_orders['instrument name'],
                   'inst price': day_instrument_orders['instrument price'],
                   'inst entry type': day_instrument_orders['instrument entry type'],
                   'inst entry time': day_instrument_orders['instrument entry time'],
                   'inst entry price': day_instrument_orders['instrument entry price'],
                   'inst entry qty': day_instrument_orders['instrument entry qty'],
                   'inst exit type': day_instrument_orders['instrument exit type'],
                   'inst exit qty': day_instrument_orders['instrument exit qty'],
                   'inst exit time': day_instrument_orders['instrument exit time'],
                   'inst exit price': day_instrument_orders['instrument exit price'],
                   'inst profit': day_instrument_orders['instrument profit'],
                   'inst profit or loss': day_instrument_orders['instrument profit or loss']}]
    telegram_message_txt = str((pd.DataFrame(automation).iloc[-1]))
    telegram_message = {'chat_id': chat_id, 'text': telegram_message_txt, 'parse_mode': 'html'}
    requests.post(api_telegram_url, json=telegram_message)


