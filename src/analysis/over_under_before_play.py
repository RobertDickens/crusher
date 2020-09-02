from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Market, Event, Team, Runner, ExchangeOddsSeries
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.division import DivisionCodeEnum
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt


with dbm.get_managed_session() as session:
    # Get data
    runner = Runner.get_by_code(session, RCEnum.UNDER_2_5_GOALS)
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    division_code=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                                   DivisionCodeEnum.CHAMPIONSHIP,
                                                                   ],
                                                    runner_uid=runner.runner_uid,
                                                    in_play=False,
                                                    item_freq_type_code=IFTCEnum.MINUTE
                                                    )

    # drop series with extreme values
    series_uids = df[df['ltp'] > df['ltp'].mean() + (3 * df['ltp'].std())].series_uid.unique()
    df = df[~df['series_uid'].isin(series_uids)]

    # drop series with less than 30 data points
    value_counts = df['series_uid'].value_counts()
    series_uids = value_counts[value_counts < 30].index
    df = df[~df['series_uid'].isin(series_uids)]

    df = df.sort_values(['series_uid', 'published_datetime'], ascending=[True, True])
    df = df[['series_uid', 'published_datetime', 'ltp']]

    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid].copy(deep=True)
        sub_df = sub_df.set_index(pd.DatetimeIndex(sub_df['published_datetime']))
        sub_df = sub_df.drop(['published_datetime', 'series_uid'], axis=1)
        sub_df = sub_df.resample('T').asfreq().fillna(method='ffill')
        sub_df.plot()
        plt.ylim([1, max(sub_df['ltp'] + 0.3)])
        plt.show()
    value_counts = df['series_uid'].value_counts().reset_index().rename(columns={'series_uid': 'item_count',
                                                                                 'index': 'series_uid'})
    means = df.groupby('series_uid').mean().rename(columns={'ltp': 'series_mean'}).reset_index()
    std_deviations = df.groupby('series_uid').std().rename(columns={'ltp': 'series_std_dev'}).reset_index()
    comparison_df = pd.merge(means, std_deviations, on='series_uid')
    comparison_df = pd.merge(comparison_df, value_counts, on='series_uid')
    comparison_df['coefficients_of_variation'] = comparison_df['series_std_dev'] / comparison_df['series_mean']
    plt.scatter(comparison_df['series_mean'], comparison_df['coefficients_of_variation'])
    plt.ylabel('standard deviation')
    plt.xlabel('mean')
    plt.title("Mean v Coefficient of variation for Premier League (Pre-kickoff)")
    plt.show()
