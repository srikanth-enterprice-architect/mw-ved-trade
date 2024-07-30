def vwap(df_bank_nifty_super_trend_7_3):
    """
        The volume weighted average price (VWAP) is a trading benchmark used especially in pension plans.
        VWAP is calculated by adding up the dollars traded for every transaction (price multiplied by number of shares traded) and then dividing
        by the total shares traded for the day.
    """
    df_bank_nifty_super_trend_7_3['vwap'] = (df_bank_nifty_super_trend_7_3.volume * (
            df_bank_nifty_super_trend_7_3.high + df_bank_nifty_super_trend_7_3.low)
                                             / 2).cumsum() / df_bank_nifty_super_trend_7_3.volume.cumsum()

    return df_bank_nifty_super_trend_7_3
