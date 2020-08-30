from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Market, Event, Team, Runner
from crusher.runner import RunnerCodeEnum as RCEnum
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt


with dbm.get_managed_session() as session:
    # Get data
    runner = Runner.get_by_code(session, RCEnum.SCORE_0_0)
    events = Event.get_by_alternate_key()
    df = ExchangeOddsSeriesItem.get_series_items_df(session, runner_uid=runner.runner_uid,
                                                    in_play=False)
    # Drop extreme values
    df = df.loc[df['ltp'] < df['ltp'].mean() + (3 * df['ltp'].std())]
    print(df['ltp'].mean())
    plt.hist(df['ltp'], bins=30)
    plt.ylabel('Count')
    plt.xlabel('ltp')
    plt.show()

