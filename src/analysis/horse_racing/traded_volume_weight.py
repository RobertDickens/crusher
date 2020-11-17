from datetime import datetime
from matplotlib import pyplot as plt

from utils.helper_functions.data_loading import get_processed_horse_racing_odds
from crusher.runner import RunnerCodeEnum as RCEnum
from orm.orm import ExchangeOddsSeriesItem, Runner, MarketType
from utils.helper_functions import preprocessing as pr
from crusher.market_type import MarketTypeCodeEnum as MTCEnum

df_ = get_processed_horse_racing_odds(runner_codes=[RCEnum.FAVOURITE, RCEnum.SECOND_FAVOURITE],
                                      from_date=datetime(2020, 1, 1), until_date=datetime(2020, 1, 2),
                                      market_location_code='CHELTENHAM')


def calculate_volume_weighted_average_price(df, ltp_col_name, tv_col_name):
    df['traded_per_second'] = df[tv_col_name].diff()
    df = df.dropna()
    vwap = (df['traded_per_second'] * df[ltp_col_name]).sum() / df['traded_per_second'].sum()
    return vwap

def calculate_average_final_price(df, final_price_range_in_seconds):
    pass

def calculate_traded_volume_by_price(df, ltp_col_name, tv_col_name):
    df['traded_per_second'] = df[tv_col_name].diff()
    df = df.dropna()
    tvbp = df[[ltp_col_name, 'traded_per_second']].groupby(ltp_col_name).sum()['traded_per_second']
    plt.plot(tvbp.index, tvbp)
    vwap = calculate_volume_weighted_average_price(df, ltp_col_name=ltp_col_name, tv_col_name=tv_col_name)
    # draw vertical line from (70,100) to (70, 250)
    plt.plot([vwap, vwap], [0, max(tvbp)], 'k-', lw=2)
    plt.show()


for series_uid in df_['series_uid'].unique():
    sub_df = df_[df_['series_uid'] == series_uid]
    calculate_traded_volume_by_price(sub_df, 'favourite_ltp', 'favourite_tv')
