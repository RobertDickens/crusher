from utils.helper_functions.calculation import green


def naive_high_odds_grabber(df, odds_increase, back_stake):
    first_seen_back_odds = df.iloc[0]['score_odds']
    back_odds = first_seen_back_odds + odds_increase
    try:
        ix, back_odds = next((i, v) for (i, v) in zip(df.index.values, df['score_odds'].values) if v >= back_odds)
    except:
        print('did not find')
        return 0

    df_after_back = df[df.index > ix]
    try:
        lay_odds = next(v for v in df_after_back['score_odds'].values if v < back_odds)
    except:
        lay_odds = df.iloc[-1]['score_odds']

    return green(back_stake=back_stake, back_odds=back_odds, lay_odds=lay_odds)