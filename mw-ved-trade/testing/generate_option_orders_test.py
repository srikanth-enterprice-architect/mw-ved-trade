import pandas as pd

from order_management.generate_option_orders import get_super_trend_results

raw_df_date = pd.read_csv('BSE_SENSEX-INDEX_5.csv')
results = get_super_trend_results(raw_df_date, period=7, multiplier=1)

inset_record_temp = 0
direction = 'down'

for inset_record_position, inset_record in results.iterrows():
    if inset_record.super_trend_direction_7_3 == 'up':
        if inset_record_temp != 0:
            if inset_record_temp.close > inset_record.close:
                direction = 'up'
        else:
            user_record_pos = inset_record_temp

print()