from orm.orm import ItemFreqType
from utils.database_manager import dbm
from utils.custom_enum import CustomEnum


class ItemFreqTypeCode(CustomEnum):
    MINUTE = 'MINUTE'
    SECOND = 'SECOND'


with dbm.get_managed_session() as session:
    session.add(ItemFreqType(item_freq_type_code=ItemFreqTypeCode.MINUTE,
                             item_freq_type_desc='minute'))
    session.add(ItemFreqType(item_freq_type_code=ItemFreqTypeCode.SECOND,
                             item_freq_type_desc='second'))
