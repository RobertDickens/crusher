from orm.orm import MarketType, Country
from utils.database_manager import dbm
from utils.custom_enum import CustomEnum


class MarketTypeCode(CustomEnum):
    CORRECT_SCORE = 'CORRECT_SCORE'


with dbm.get_managed_session() as session:
    session.add(MarketType(market_type_code=MarketTypeCode.CORRECT_SCORE,
                           market_type_desc='correct final match score'))
