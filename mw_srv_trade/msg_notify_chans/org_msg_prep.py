from datetime import datetime, date
import pandas as pd

from mw_srv_trade.comm_ops.common_ops import multi_order_qty_normal_instagram
from mw_srv_trade.constants.file_constants import DAY_INSTRUMENT_ORDERS


def client_capital_info(client_capital_book, day_instrument_orders):
    client_capital = client_capital_book.iloc[-1].client_capital
    current_capital = client_capital_book.iloc[-1].client_capital
    if day_instrument_orders.shape[0] > 0:
        current_capital = client_capital_book.iloc[-1].client_current_capital
    return client_capital, current_capital


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
