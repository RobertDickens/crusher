import pandas as pd
import numpy as np
from crusher.market_location import MarketLocationCodeEnum as MLCEnum
from orm import orm
from utils.db.database_manager import dbm
from matplotlib import pyplot as plt
from crusher.runner import RunnerCodeEnum as RCEnum
from utils.helper_functions.data_loading import get_processed_horse_racing_odds
from datetime import datetime


# Get average volume matched
df = get_processed_horse_racing_odds(runner_codes=[RCEnum.FAVOURITE,
                                                   RCEnum.SECOND_FAVOURITE,
                                                   RCEnum.THIRD_FAVOURITE],
                                     market_location_code=MLCEnum.NEWCASTLE,
                                     min_market_pre_off_volume=100000,
                                     from_date=datetime(2020, 1, 1),
                                     until_date=datetime(2020, 4, 1))
df.to_csv('C:\\Users\\rober\\crusher\\analysis\\newcastle.csv', index=False)

