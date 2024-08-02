from os import path
import requests

from mw_minisoft.common_operatinos.common_ops import multi_order_qty_normal_instagram
from mw_minisoft.instruments_operations.instrument_read_write_operations import read_instrument_tokens
from mw_minisoft.persistence_operations.account_management import *


# https://my.telegram.org/apps
# https://medium.com/javarevisited/sending-a-message-to-a-telegram-channel-the-easy-way-eb0a0b32968
# https://www.shellhacks.com/python-send-message-to-telegram/#:~:text=Send%20Message%20to%20Telegram%20using%20Python&text=post(apiURL%2C%20json%3D%7B%27chat_id,%22Hello%20from%20Python!%22)&text=You%20can%20also%20send%20images,through%20the%20API%20using%20Python.
# algo_trading_stg_01 - all instruments
#
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


def client_capital_book_preparation(client_capital_book, inst_record, inst_order_record, sp_user_session):
    instrument_exit_price = common_params(sp_user_session, inst_order_record)
    client_current_capital = client_capital_book.iloc[-1].client_current_capital
    client_current_capital = client_current_capital + (inst_record.default_quantity * instrument_exit_price)
    client_capital_book.at[0, 'client_current_capital'] = client_current_capital


def exit_order_preparation(client_capital_book, inst_record, inst_order_record, sp_user_session, strategy_name):
    columns = ["instrument entry type", "instrument exit type", "instrument entry time", "instrument exit time"]
    day_instrument_orders = pd.read_csv(DAY_INSTRUMENT_ORDERS)
    day_instrument_orders[columns] = day_instrument_orders[columns].astype(str)

    day_inst_orders = day_instrument_orders[
        (day_instrument_orders['instrument entry type'] == str(inst_order_record['inst_option_name']))]
    day_inst_orders = day_inst_orders[day_inst_orders['strategy_name'] == strategy_name]
    day_inst_orders = day_inst_orders[day_inst_orders['instrument exit price'].isna()].tail(1)

    day_inst_orders_ = day_inst_orders.iloc[-1]
    day_inst_orders_index = day_inst_orders.index.values[0]

    instrument_exit_price = common_params(sp_user_session, inst_order_record)

    client_current_capital = client_capital_book.iloc[-1].client_current_capital
    client_current_capital = client_current_capital + (day_inst_orders_['instrument entry qty'] * instrument_exit_price)

    day_instrument_orders.at[day_inst_orders_index, 'instrument exit type'] = day_inst_orders_['instrument entry type']
    day_instrument_orders.at[day_inst_orders_index, 'instrument exit qty'] = day_inst_orders_['instrument entry qty']
    day_instrument_orders.at[day_inst_orders_index, 'instrument exit time'] = datetime.now().strftime('%H:%M:%S')
    day_instrument_orders.at[day_inst_orders_index, 'instrument exit price'] = instrument_exit_price
    day_instrument_orders.at[day_inst_orders_index, 'client current capital'] = client_current_capital

    entry_price = day_instrument_orders.at[day_inst_orders_index, 'instrument entry price']
    exit_price = day_instrument_orders.at[day_inst_orders_index, 'instrument exit price']
    profit_loss = (entry_price - exit_price)
    inst_profit_loss = -(profit_loss * day_inst_orders_['instrument entry qty'])
    day_instrument_orders.at[day_inst_orders_index, 'instrument profit or loss'] = inst_profit_loss

    return day_instrument_orders, day_inst_orders_index


def client_capital_info(client_capital_book, day_instrument_orders):
    client_capital = client_capital_book.iloc[-1].client_capital
    current_capital = client_capital_book.iloc[-1].client_capital
    if day_instrument_orders.shape[0] > 0:
        current_capital = client_capital_book.iloc[-1].client_current_capital
    return client_capital, current_capital


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


def common_params(sp_user_session, inst_order_record):
    inst_name = inst_order_record['inst_option_name']
    inst_data = {"symbols": inst_name}
    return (sp_user_session.quotes(inst_data))['d'][0]['v']['lp']


def day_order_dict_pre(inst_record, inst_order_record, client_capital_book, day_instrument_orders, sp_user_session,
                       strategy_name):
    instrument_entry_price = common_params(sp_user_session, inst_order_record)
    client_capital, current_capital = client_capital_info(client_capital_book, day_instrument_orders)

    multi_order_qty_ = multi_order_qty_normal_instagram(inst_record)

    return [{'instrument date': date.today(), 'strategy_name': strategy_name, 'client Initial capital': client_capital,
             'client current capital': current_capital - (multi_order_qty_ * instrument_entry_price),
             'instrument name': inst_order_record['inst_name'], 'instrument price': inst_order_record['inst_price'],
             'instrument entry type': inst_order_record['inst_option_name'],
             'instrument entry time': datetime.now().strftime("%H:%M:%S"),
             'instrument entry price': instrument_entry_price,
             'instrument entry qty': multi_order_qty_,
             'instrument exit type': '', 'instrument exit qty': '', 'instrument exit time': '',
             'instrument exit price': '', 'instrument profit': ''}]
