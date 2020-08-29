from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Market, Event, Team, Runner
from crusher.runner import RunnerCodeEnum as RCEnum

import pandas as pd

with dbm.get_managed_session() as session:
    runner = Runner.get_by_code(session, RCEnum.SCORE_0_0)
    df = ExchangeOddsSeriesItem.get_series_items_df(session, runner_uid=runner.runner_uid,
                                                    in_play=True)
    print(df)
