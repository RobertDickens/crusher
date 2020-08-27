from orm.orm import InfoSource, InfoSourceOrganisation
from crusher.info_source_organisation import InfoSourceOrganisationEnum as ISOEnum
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class InfoSourceEnum(CustomEnum):
    EXCHANGE_HISTORICAL = 'EXCHANGE_HISTORICAL'


def create_or_update_info_source():
    with dbm.get_managed_session() as session:
        info_source_orgn = InfoSourceOrganisation.get_by_name(session, ISOEnum.BETFAIR)
        InfoSource.create_or_update(session, info_source_code=InfoSourceEnum.EXCHANGE_HISTORICAL,
                                    info_source_name='historical exchange data',
                                    info_source_organisation=info_source_orgn)


if __name__ == '__main__':
    create_or_update_info_source()
