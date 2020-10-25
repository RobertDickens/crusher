import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from dateutil.relativedelta import relativedelta
from orm.orm import ExchangeOddsSeriesItem, Market, Runner, Event, ExchangeOddsSeries
from utils.db.database_manager import dbm
from crusher.sport import SportCodeEnum as SCEnum
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.market_type import MarketTypeCodeEnum as MTCEnum

# Get win markets where preoff volume > 100k
with dbm.get_managed_session() as session:
    raw_df = ExchangeOddsSeriesItem.get_series_items_df(session, market_type_code=MTCEnum.WIN,
                                                        in_play=False, min_market_pre_off_volume=1000000,
                                                        runner_codes=[RCEnum.FAVOURITE])
    # Create features
    df = pd.DataFrame(columns=['volume_delta', 'ltp', 'ltp_delta', 'min_to_off'])
    for series_uid in raw_df['series_uid'].unique():
        off_time = ExchangeOddsSeries.get_by_uid(session, int(series_uid)).market.off_time
        sub_df = raw_df.loc[raw_df['series_uid'] == series_uid]
        sub_df = sub_df.loc[sub_df['published_datetime'] > off_time - relativedelta(minutes=30)]
        sub_df = sub_df.sort_values('published_datetime', ascending=True)
        sub_df['volume_delta'] = sub_df['traded_volume'].diff()
        sub_df['ltp_delta'] = sub_df['ltp'].diff()
        sub_df = sub_df.dropna()
        sub_df['min_to_off'] = sub_df['published_datetime'].apply(lambda x: (x - off_time).total_seconds() / 60.0)
        sub_df = sub_df.set_index(['published_datetime'])
        # create figure and axis objects with subplots()
        fig, ax = plt.subplots()
        ax.plot(sub_df.index, sub_df.ltp, color="red", marker="o")
        ax.set_xlabel("time", fontsize=14)
        ax.set_ylabel("ltp", color="red", fontsize=14)
        ax2 = ax.twinx()
        ax2.plot(sub_df.index, sub_df["volume_delta"], color="blue", marker="o")
        ax2.set_ylabel("traded_volume", color="blue", fontsize=14)
        plt.show()
        df = df.append(sub_df)
    del raw_df

    print(df)
