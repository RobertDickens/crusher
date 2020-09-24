from utils.helper_functions.calculation import green


def naive_high_odds_grabber_by_ticks(df, odds_column_name, odds_increase, back_stake):
    """Tries to back at higher odds than ltp. If it can back, greens out at first
    opportunity. If not """
    first_seen_back_odds = df.iloc[0][odds_column_name]
    back_odds = first_seen_back_odds + odds_increase
    try:
        ix, back_odds = next((i, v) for (i, v) in zip(df.index.values, df[odds_column_name].values) if v >= back_odds)
    except:
        return 0

    df_after_back = df[df.index > ix]
    try:
        lay_odds = next(v for v in df_after_back[odds_column_name].values if v < back_odds)
    except:
        lay_odds = df.iloc[-1][odds_column_name]

    return green(back_stake=back_stake, back_odds=back_odds, lay_odds=lay_odds)


def naive_high_odds_grabber_by_percentage(df, odds_column_name, odds_percentage, back_stake):
    """Tries to back at higher odds than ltp. If it can back, greens out at first
    opportunity. If not """
    first_seen_back_odds = df.iloc[0][odds_column_name]
    back_odds = first_seen_back_odds * (odds_percentage/100)
    try:
        ix, back_odds = next((i, v) for (i, v) in zip(df.index.values, df[odds_column_name].values) if v >= back_odds)
    except:
        return 0

    df_after_back = df[df.index > ix]
    try:
        lay_odds = next(v for v in df_after_back[odds_column_name].values if v < back_odds)
    except:
        lay_odds = df.iloc[-1][odds_column_name]
        green(back_stake=back_stake, back_odds=back_odds, lay_odds=lay_odds)

    return green(back_stake=back_stake, back_odds=back_odds, lay_odds=lay_odds)