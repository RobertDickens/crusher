from orm.orm import MarketType
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class MarketTypeCodeEnum(CustomEnum):
    CORRECT_SCORE = 'CORRECT_SCORE'
    OVER_UNDER_GOALS = 'OVER_UNDER_GOALS'


def create_or_update_market_type():
    with dbm.get_managed_session() as session:
        MarketType.create_or_update(session, market_type_code=MarketTypeCodeEnum.CORRECT_SCORE,
                                    market_type_desc='correct final match score')
        MarketType.create_or_update(session, market_type_code=MarketTypeCodeEnum.OVER_UNDER_GOALS,
                                    market_type_desc='correct final match score')


if __name__ == '__main__':
    create_or_update_market_type()
