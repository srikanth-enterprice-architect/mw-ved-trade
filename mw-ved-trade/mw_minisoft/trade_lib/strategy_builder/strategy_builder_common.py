def exit_entry_time(instrument_name):
    exit_time = '15:10:00'
    if ('NSE' in instrument_name) & ('NIFTY' in instrument_name):
        exit_time = '15:10:00'
    elif ('NSE' in instrument_name) & ('INR' in instrument_name):
        exit_time = '16:40:00'
    elif 'MCX' in instrument_name:
        exit_time = '23:00:00'
    return exit_time


def update_record_before_exit_time_strategy(current_day_data, instr_positional_orders, row_number, row_record,
                                            instr_days_data):
    instr_days_data.loc[row_number, 'day_open_strategy'] = instr_days_data.loc[row_number - 1, 'day_open_strategy']


def update_record_after_exit_time_strategy(current_day_data, instr_positional_orders, row_number, instr_days_data):
    day_first_orders = instr_positional_orders.loc[0]
    if 'up' in str(day_first_orders.day_open_strategy):
        instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_exit'
        row_number_update = instr_days_data.loc[row_number]
        instr_positional_orders = instr_positional_orders.append(row_number_update, ignore_index=True)
    elif 'down' in str(day_first_orders.day_open_strategy):
        instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_exit'
        row_number_update = instr_days_data.loc[row_number]
        instr_positional_orders = instr_positional_orders.append(row_number_update, ignore_index=True)
    return instr_positional_orders


def instrument_day_orders(instr_day_orders, row_record, current_record_direction, previous_record_direction):
    if current_record_direction != previous_record_direction:
        instr_day_orders = instr_day_orders.append(row_record, ignore_index=True)
    return instr_day_orders


def collect_day_specific_data(instr_days_data, instr_days_record):
    instr_days_data = instr_days_data[(instr_days_data['date_on_str'] == instr_days_record.days)]
    return instr_days_data
