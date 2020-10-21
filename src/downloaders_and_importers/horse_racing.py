import os

from utils.db.database_manager import dbm
from utils.parsers.horse_racing_odds_parser import HorseRacingExchangeOddsExtractor


for month in ['Jan', 'Feb', 'Mar']:
    root_dir = "C:\\Users\\rober\\sport_data\\horse_racing\\ADVANCED\\2020\\"
    root_dir = os.path.join(root_dir, month)
    for subdir, dirs, files in os.walk(root_dir):
        with dbm.get_managed_session() as session:
            for file in files:
                try:
                    extractor = HorseRacingExchangeOddsExtractor(os.path.join(subdir, file))
                    event_data, market_data, odds, volume = extractor.extract_data()
                    pre_off_volume = volume.loc[volume.index < event_data['off_time']]
                    total_pre_off_volume = pre_off_volume.max(axis=0).sum()
                    print(total_pre_off_volume)
                except Exception as exp:
                    print(exp)
