import os

from utils.db.database_manager import dbm
from utils.parsers.horse_racing_odds_parser import HorseRacingExchangeOddsExtractor
from orm.orm import Event, Market, ExchangeOddsSeries, MarketLocation
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from crusher.sport import SportCodeEnum as SCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.info_source import InfoSourceEnum as ISEnum
from utils.db import db_table_names as tb


def import_horse_racing_data():
    n_rows = 0
    for month in ['Mar']:
        root_dir = "C:\\Users\\rober\\sport_data\\horse_racing\\ADVANCED\\2020\\"
        root_dir = os.path.join(root_dir, month)
        for subdir, dirs, files in os.walk(root_dir):
            for file in files:
                try:
                    with dbm.get_managed_session() as session:
                        print(subdir)
                        extractor = HorseRacingExchangeOddsExtractor(os.path.join(subdir, file))
                        event_data, market_data, df, total_pre_off_volume, total_volume = extractor.extract_data()
                        event, _ = Event.create_or_update(session, event_data['event_id'], team_a=None, team_b=None,
                                                          sport_code=SCEnum.HORSE_RACING)
                        market_location_name = market_data['venue'].lower()
                        market_location_code = market_data['venue'].upper()
                        market_location_code = market_location_code.replace(' ', '_')
                        MarketLocation.create_or_update(session,
                                                        market_location_code=market_location_code,
                                                        market_location_name=market_location_name)
                        market, existed = Market.create_or_update(session,
                                                                  market_betfair_id=str(market_data['market_id']),
                                                                  market_type_code=MTCEnum.WIN, event=event,
                                                                  pre_off_volume=total_pre_off_volume,
                                                                  total_volume=total_volume,
                                                                  off_time=event_data['off_time'],
                                                                  market_location_code=market_location_code)
                        series, _ = ExchangeOddsSeries.create_or_update(session, event=event, market=market,
                                                                        item_freq_type_code=IFTCEnum.SECOND,
                                                                        info_source_code=ISEnum.EXCHANGE_HISTORICAL)
                        df['series_uid'] = series.series_uid
                        df.to_sql(name=tb.exchange_odds_series_item(), schema='public',
                                  con=session.connection(),
                                  if_exists='append', method='multi', index=False)
                        n_rows += len(df)
                        print(f"Added {len(df)} to db for total of {n_rows}")
                except Exception as exp:
                    print(exp)


if __name__ == '__main__':
    import_horse_racing_data()
