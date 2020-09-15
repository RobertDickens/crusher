import pandas as pd
from orm.orm import ExchangeOddsSeries
from utils.db.database_manager import dbm


def drop_series_with_price_jumps(df, threshold, verbose=True):
    """Drops series that contain price jumps above a given threshold. Useful for
    removing series with goals
    """
    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid].copy(deep=True)
        if any(sub_df['ltp'].diff() > threshold):
            df = df[df['series_uid'] != series_uid]

    return df


def drop_outlier_series_by_std(df, std_threshhold=3, value_col='ltp', verbose=True):
    """Drops series that contain values that are more than 3x the standard
    deviation of the entire dataframe"""
    value_mean = df[value_col].mean()
    value_std = df[value_col].std()
    mask = df[value_col] > value_mean + (std_threshhold * value_std)
    series_uids_to_drop = df.loc[mask]['series_uid'].unique()
    df = df[~df['series_uid'].isin(series_uids_to_drop)]

    return df


def convert_published_datetime_to_game_time(df):
    with dbm.get_managed_session() as session:
        in_play_map = {series_uid: ExchangeOddsSeries.get_by_uid(session, int(series_uid)).event.in_play_start
                       for series_uid in df['series_uid'].unique()}
    new_df = pd.DataFrame(columns=['series_uid', 'game_time', 'ltp'])
    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid].copy(deep=True)
        sub_df = sub_df.set_index(pd.DatetimeIndex(sub_df['published_datetime']))
        sub_df = sub_df.drop(['published_datetime', 'series_uid'], axis=1)
        sub_df = sub_df.resample('T').asfreq().fillna(method='ffill')
        sub_df = sub_df.reset_index()
        sub_df['series_uid'] = series_uid
        sub_df['in_play_time'] = sub_df['series_uid'].map(in_play_map)
        sub_df['game_time'] = (sub_df['published_datetime'] - sub_df['in_play_time']).apply(lambda x: x.seconds / 60)
        sub_df = sub_df.drop(['in_play_time', 'published_datetime'], axis=1)
        if sub_df.iloc[0]['game_time'] != 0:
            initial_times = range(0, int(sub_df.iloc[0]['game_time']))
            rows_to_insert = {'series_uid': [series_uid] * len(initial_times),
                              'ltp': [sub_df.iloc[0]['ltp']] * len(initial_times),
                              'game_time': list(initial_times)}
            sub_df = pd.concat([pd.DataFrame(rows_to_insert), sub_df])
        new_df = pd.concat([new_df, sub_df], ignore_index=True)

    df = new_df
    return df


def drop_series_by_n_data_points(df, threshold):
    """Drops all series where number of data points is less than given threshold"""
    value_counts = df['series_uid'].value_counts()
    series_uids = value_counts[value_counts < threshold].index
    df = df[~df['series_uid'].isin(series_uids)]
    return df


def normalise_in_play_ltp(df):
    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid]
        initial_value = sub_df.iloc[0]['ltp']
        df.at[sub_df.index, 'normalised_ltp'] = df.loc[sub_df.index, 'ltp'] / initial_value
    return df

