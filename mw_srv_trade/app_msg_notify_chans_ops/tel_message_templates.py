import pandas as pd


def build_tel_notify_message(day_instrument_orders):
    # Construct the message dynamically using the provided data parameter
    telegram_message_txt = {
        "Traded Date":          day_instrument_orders['traded date'],
        "Invested Amount":      day_instrument_orders['investment amount'],
        "Current Amount":       day_instrument_orders['current amount'],
        "Instrument Buy Name":  day_instrument_orders['instrument option name'],
        "Instrument Buy Price": day_instrument_orders['option buy price'],
        "Instrument Buy Qty":   day_instrument_orders['option buy qty'],
        "Instrument Buy Time":  day_instrument_orders['option buy time'],
        "Instrument Sell Price":day_instrument_orders['option sell price'],
        "Instrument Sell Qty":  day_instrument_orders['option sell qty'],
        "Instrument Sell Time": day_instrument_orders['option sell time'],
        "Instrument Gains":     day_instrument_orders['option profit or loss']
    }
    telegram_message_txt = pd.DataFrame([telegram_message_txt])
    return telegram_message_txt
