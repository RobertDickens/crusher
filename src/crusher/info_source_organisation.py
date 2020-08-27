from orm.orm import InfoSourceOrganisation
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class InfoSourceOrganisationEnum(CustomEnum):
    BETFAIR = 'BETFAIR'


def create_or_update_info_source_organisation():
    with dbm.get_managed_session() as session:
        InfoSourceOrganisation.create_or_update(session, organisation_name=InfoSourceOrganisationEnum.BETFAIR,
                                                organisation_url='www.betfair.co.uk')


if __name__ == '__main__':
    create_or_update_info_source_organisation()
