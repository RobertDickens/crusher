from orm.orm import MarketType
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class DivisionCodeEnum(CustomEnum):
    PREMIER_LEAGUE = 'CORRECT_SCORE'
    CHAMPIONSHIP = 'OVER_UNDER_GOALS'
    LEAGUE_1 = 'LEAGUE_1'
    LEAGUE_2 = 'LEAGUE_2'
    CONFERENCE = 'CONFERENCE'


def create_or_update_market_type():
    with dbm.get_managed_session() as session:
        MarketType.create_or_update(session, market_type_code=MarketTypeCodeEnum.CORRECT_SCORE,
                                    market_type_desc='correct final match score')
        MarketType.create_or_update(session, market_type_code=MarketTypeCodeEnum.OVER_UNDER_GOALS,
                                    market_type_desc='correct final match score')


if __name__ == '__main__':
    create_or_update_market_type()
