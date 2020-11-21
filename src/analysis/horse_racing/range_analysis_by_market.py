import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib import pyplot as plt

from crusher.runner import RunnerCodeEnum as RCEnum
from utils.helper_functions.data_loading import get_processed_horse_racing_odds
from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeries, Market
from crusher.market_location import MarketLocationCodeEnum as MLCEnun
from analysis.horse_racing.traded_volume_weight import calculate_volume_weighted_average_price


# Load processed dfs for market
df = get_processed_horse_racing_odds(runner_codes=[RCEnum.FAVOURITE,
                                                   RCEnum.SECOND_FAVOURITE,
                                                   RCEnum.THIRD_FAVOURITE],
                                     market_location_code=MLCEnun.NEWCASTLE,
                                     from_date=datetime(2020, 1, 1),
                                     until_date=datetime(2020, 1, 10))
vwav_before_3_min = []
vwav_after_3_min = []
price_at_3_min = []
final_prices = []

with dbm.get_managed_session() as session:
    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid]
        # Subset by <3 and >3 minutes from off

        off_time = ExchangeOddsSeries.get_by_uid(session, int(series_uid)).market.off_time
        sub_df = sub_df.set_index('published_datetime')
        sub_df_x = sub_df[(off_time - sub_df.index).seconds > 180]
        sub_df_y = sub_df[(off_time - sub_df.index).seconds <= 180]
        x_vwav = calculate_volume_weighted_average_price(sub_df_x, 'favourite_ltp', 'favourite_tv')
        y_vwav = calculate_volume_weighted_average_price(sub_df_y, 'favourite_ltp', 'favourite_tv')
        x_price = sub_df[((off_time - sub_df.index).seconds > 175) & ((off_time - sub_df.index).seconds < 185)]['favourite_ltp'].mean()
        final_price = sub_df[(off_time - sub_df.index).seconds < 20]['favourite_ltp'].mean()

        vwav_before_3_min.append(x_vwav)
        vwav_after_3_min.append(y_vwav)
        price_at_3_min.append(x_price)
        final_prices.append(final_price)
        #
        # sub_df['favourite_ltp'].plot()
        # plt.show()


    analysis_df = pd.DataFrame({'vwav_before_3': vwav_before_3_min,
                                'vwav_after_3': vwav_after_3_min,
                                'price_at_3_min': price_at_3_min,
                                'final_price': final_prices}
                               )

    print(analysis_df)

# Calculate volume weighted average price for > 3 mins from off
# Calculate volume weighted average price for < 3 mins from off
# Calculate average price in final 20-10 seconds from off
