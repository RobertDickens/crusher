import pandas as pd


def drop_series_with_price_jumps(df, threshold):
    """Drops series that contain price jumps above a given threshold. Useful for
    removing series with goals"""
    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid].copy(deep=True)
        if any(sub_df['ltp'].diff() > 0.2):
            df = df[df['series_uid'] != series_uid]

    return df

# Get and clean data
from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Market, Event, Team, Runner, ExchangeOddsSeries
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.division import DivisionCodeEnum
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt
from dateutil.relativedelta import relativedelta

with dbm.get_managed_session() as session:
    # Get data
    runner = Runner.get_by_code(session, RCEnum.UNDER_2_5_GOALS)
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    division_code=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                                   DivisionCodeEnum.CHAMPIONSHIP,
                                                                   ],
                                                    runner_uid=runner.runner_uid,
                                                    in_play=True,
                                                    item_freq_type_code=IFTCEnum.MINUTE
                                                    )

    # drop series with extreme values
    series_uids = df[df['ltp'] > df['ltp'].mean() + (3 * df['ltp'].std())].series_uid.unique()
    df = df[~df['series_uid'].isin(series_uids)]

    # drop series with less than 20 data points
    value_counts = df['series_uid'].value_counts()
    series_uids = value_counts[value_counts < 30].index
    df = df[~df['series_uid'].isin(series_uids)]

    in_play_map = {series_uid: ExchangeOddsSeries.get_by_uid(session, int(series_uid)).event.in_play_start
                   for series_uid in df['series_uid'].unique()}
    df = df.sort_values(['series_uid', 'published_datetime'], ascending=[True, True])

    df = df[['series_uid', 'ltp', 'published_datetime']]
    # Get start time from event data
    df = drop_series_with_price_jumps(df, threshold=0.2)

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
        sub_df = sub_df[sub_df['game_time'] < 30]
        sub_df = sub_df.drop(['in_play_time', 'published_datetime'], axis=1)
        if sub_df.iloc[0]['game_time'] != 0:
            initial_times = range(0, int(sub_df.iloc[0]['game_time']))
            rows_to_insert = {'series_uid': [series_uid] * len(initial_times),
                              'ltp': [sub_df.iloc[0]['ltp']] * len(initial_times),
                              'game_time': list(initial_times)}
            sub_df = pd.concat([pd.DataFrame(rows_to_insert), sub_df])
        new_df = pd.concat([new_df, sub_df], ignore_index=True)

    print(new_df)
