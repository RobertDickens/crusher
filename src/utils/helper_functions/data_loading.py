from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner
from utils.helper_functions import preprocessing as pr


def get_odds_data(runner, divisions, in_play, item_freq_type_code,
                  convert_to_game_time=False):
    with dbm.get_managed_session() as session:
        runner = Runner.get_by_code(session, runner)
        df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                        division_code=divisions,
                                                        runner_uid=runner.runner_uid,
                                                        in_play=in_play,
                                                        item_freq_type_code=item_freq_type_code)
        df = df.sort_values(['series_uid', 'published_datetime'], ascending=True).reset_index(drop=True)
        if convert_to_game_time:
            df = pr.convert_published_datetime_to_game_time(df)
            df = df[['series_uid', 'game_time', 'ltp']]
        df = df[['series_uid', 'published_datetime', 'ltp']]
        return df
