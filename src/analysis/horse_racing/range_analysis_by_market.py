import pandas as pd
from datetime import datetime

from crusher.runner import RunnerCodeEnum as RCEnum
from utils.helper_functions.data_loading import get_processed_horse_racing_odds
from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeries
from crusher.market_location import MarketLocationCodeEnum as MLCEnum
from analysis.horse_racing.traded_volume_weight import calculate_volume_weighted_average_price


def get_metrics_from_df(df, off_time, cut_off_seconds, runner_code):
    """Calculate various metrics"""
    df = df.set_index('published_datetime')
    ltp_col_name = runner_code.lower() + '_ltp'
    tv_col_name = runner_code.lower() + '_tv'
    df_before_cut_off = df.loc[(off_time - df.index).seconds > cut_off_seconds]
    df_after_cut_off = df.loc[(off_time - df.index).seconds <= cut_off_seconds]
    before_cut_off_upper_bound = max(df_before_cut_off[ltp_col_name])
    before_cut_off_lower_bound = min(df_before_cut_off[ltp_col_name])
    before_cut_off_trading_range = before_cut_off_upper_bound - before_cut_off_lower_bound
    after_cut_off_upper_bound = max(df_after_cut_off[ltp_col_name])
    after_cut_off_lower_bound = min(df_after_cut_off[ltp_col_name])
    after_cut_off_trading_range = after_cut_off_upper_bound - after_cut_off_lower_bound

    before_vwav = calculate_volume_weighted_average_price(df_before_cut_off, ltp_col_name, tv_col_name)
    price_at_cut_off = sub_df[((off_time - df.index).seconds > (cut_off_seconds + 5) &
                               ((off_time - df.index).seconds < (cut_off_seconds - 5)))][ltp_col_name].mean()
    final_price = sub_df[(off_time - df.index).seconds < 20][ltp_col_name].mean()

    metrics = {'before_cut_off_trading_range': before_cut_off_trading_range,
               'after_cut_off_trading_range': after_cut_off_trading_range,
               'before_vwav': before_vwav,
               'price_at_cut_off': price_at_cut_off,
               'final_price': final_price}

    return metrics


markets = []
runners = []
before_cut_off_ranges = []
after_cut_off_ranges = []
before_vwavs = []
prices_at_cut_off = []
final_prices = []
cut_off_times = []

for cut_off_time in [180, 120, 90, 60]:
    for location_code in MLCEnum.to_dict().values():
        with dbm.get_managed_session() as session:
            # Load processed dfs for market
            df_ = get_processed_horse_racing_odds(runner_codes=[RCEnum.FAVOURITE,
                                                                RCEnum.SECOND_FAVOURITE,
                                                                RCEnum.THIRD_FAVOURITE],
                                                  market_location_code=location_code,
                                                  from_date=datetime(2020, 1, 1),
                                                  until_date=datetime(2020, 4, 1))
            if df_.empty:
                continue
            for series_uid in df_['series_uid'].unique():
                print(f'Processing series {series_uid}')
                sub_df = df_.loc[df_['series_uid'] == series_uid]
                off_time_ = ExchangeOddsSeries.get_by_uid(session, int(series_uid)).market.off_time
                for runner_code_ in [RCEnum.FAVOURITE, RCEnum.SECOND_FAVOURITE, RCEnum.THIRD_FAVOURITE]:
                    metrics_ = get_metrics_from_df(sub_df, off_time_, cut_off_seconds=cut_off_time, runner_code=runner_code_)
                    markets.append(location_code)
                    runners.append(runner_code_)
                    cut_off_times.append(cut_off_time)
                    before_cut_off_ranges.append(metrics_['before_cut_off_trading_range'])
                    after_cut_off_ranges.append(metrics_['after_cut_off_trading_range'])
                    before_vwavs.append(metrics_['before_vwav'])
                    prices_at_cut_off.append(metrics_['price_at_cut_off'])
                    final_prices.append(metrics_['final_price'])

analysis_df = pd.DataFrame({'market_location': markets,
                            'runner': runners,
                            'before_cut_off_range': before_cut_off_ranges,
                            'after_cut_off_range': after_cut_off_ranges,
                            'before_vwav': before_vwavs,
                            'price_at_cut_off': prices_at_cut_off,
                            'final_price': final_prices,
                            'cut_off_time': cut_off_times})

analysis_df.to_csv('C:\\Users\\rober\\crusher\\analysis\\market_variation.csv', index=False)
