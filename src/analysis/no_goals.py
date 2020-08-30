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
    runner = Runner.get_by_code(session, RCEnum.SCORE_0_0)
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    division_code=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                                   DivisionCodeEnum.CHAMPIONSHIP,
                                                                   ],
                                                    runner_uid=runner.runner_uid,
                                                    in_play=False,
                                                    item_freq_type_code=IFTCEnum.MINUTE
                                                    )

    # # Drop extreme values
    df = df.loc[df['ltp'] < df['ltp'].mean() + (3 * df['ltp'].std())]
    df = df[['series_uid', 'ltp']]
    # Get mean
    means = df.groupby('series_uid').mean().rename(columns={'ltp': 'series_mean'}).reset_index()
    std_deviations = df.groupby('series_uid').std().rename(columns={'ltp': 'series_std_dev'}).reset_index()
    comparison_df = pd.merge(means, std_deviations, on='series_uid')
    comparison_df['coefficients_of_variation'] = comparison_df['series_std_dev'] / comparison_df['series_mean']
    plt.scatter(comparison_df['series_mean'], comparison_df['coefficients_of_variation'])
    plt.ylabel('standard deviation')
    plt.xlabel('mean')
    plt.title("Mean v Coefficient of variation for Premier League (Pre-kickoff)")
    plt.show()

