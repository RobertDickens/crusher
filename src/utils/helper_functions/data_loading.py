from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner
from utils.helper_functions import preprocessing as pr


# Modelling P/L from under 2.5 goals
def get_odds_data(runner, divisions, in_play, item_freq_type_code,
                  preprocess=True):
    with dbm.get_managed_session() as session:
        runner = Runner.get_by_code(session, runner)
        df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                        division_code=divisions,
                                                        runner_uid=runner.runner_uid,
                                                        in_play=in_play,
                                                        item_freq_type_code=item_freq_type_code)
        if preprocess:
            df = df.sort_values(['series_uid', 'published_datetime'], ascending=True)
            df = pr.convert_published_datetime_to_game_time(df)
            df = df[['series_uid', 'game_time', 'ltp']]
            return df
        else:
            return df

