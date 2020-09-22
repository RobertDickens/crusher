import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta


class BacktestStrategy:
    def __init__(self, odds_df, subset_col_name='event_uid', strategy=None,
                 initial_bankroll=None):
        self.odds_df = odds_df
        self.subset_col_name = subset_col_name
        self.strategy = strategy
        self.results = None
        self.initial_bankroll = initial_bankroll

    def run_strategy(self, strategy, start_strategy_offset=None, **kwargs):
        bankroll_time_series = [self.initial_bankroll]
        new_bankroll = self.initial_bankroll
        for subset in self.odds_df[self.subset_col_name].unique():
            sub_df = self.odds_df[self.odds_df[self.subset_col_name] == subset]
            sub_df = sub_df.sort_index(ascending=True)

            if start_strategy_offset:
                last_datetime = max(sub_df.index)
                start_datetime = last_datetime - relativedelta(**start_strategy_offset)
                sub_df = sub_df[sub_df.index > start_datetime]

            profit_or_loss = strategy(df=sub_df, **kwargs)
            new_bankroll = bankroll_time_series[-1] + profit_or_loss
            bankroll_time_series.append(new_bankroll)
        self.results = {'bankroll_time_series': bankroll_time_series,
                        'final_bankroll':  new_bankroll,
                        'profit': new_bankroll - self.initial_bankroll}
