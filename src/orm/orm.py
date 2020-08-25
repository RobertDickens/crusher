from datetime import datetime

from sqlalchemy import Column, Integer, String, Sequence, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

import utils.db_table_names as tb

Base = declarative_base()


class Country(Base):
    __tablename__ = tb.country()

    country_uid = Column(Integer, Sequence(tb.country() + '_uid_seq', schema='public'),
                         primary_key=True)
    country_name = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    team = relationship('Team', back_populates='country')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_name(cls, session, country_name):
        return session.query(cls).filter_by(country_name=country_name)


class Event(Base):
    __tablename__ = tb.event()

    event_uid = Column(Integer, Sequence(tb.event() + '_uid_seq', schema='public'),
                       primary_key=True)
    event_betfair_id = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    market = relationship("Market", back_populates="event")
    exchange_odds_series = relationship('ExchangeOddsSeries', back_populates='event')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_betfair_id(cls, session, betfair_id):
        return session.query(cls).filter_by(event_id=betfair_id)


class Market(Base):
    __tablename__ = tb.market()

    market_uid = Column(Integer, Sequence(tb.market() + '_uid_seq', schema='public'),
                        primary_key=True)
    event_uid = Column(Integer, ForeignKey(Event.event_uid))
    market_betfair_id = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    event = relationship("Event", back_populates="market")
    exchange_odds_series = relationship('ExchangeOddsSeries', back_populates='market')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_betfair_id(cls, session, betfair_id):
        return session.query(cls).filter_by(event_id=betfair_id)


class Team(Base):
    __tablename__ = tb.team()

    team_uid = Column(Integer, Sequence(tb.team() + '_uid_seq', schema='public'),
                      primary_key=True)
    team_name = Column(String)
    country_uid = Column(Integer, ForeignKey(Country.country_uid))
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    country = relationship('Country', back_populates='team')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_team_name(cls, session, team_name):
        return session.query(cls).filter_by(team_name=team_name)


class Runner(Base):
    __tablename__ = tb.runner()

    runner_uid = Column(Integer, Sequence(tb.runner() + '_uid_seq', schema='public'),
                        primary_key=True)
    runner_code = Column(String)
    runner_betfair_code = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_code(cls, session, runner_code):
        return session.query(cls).filter_by(runner_code=runner_code)


class InfoSourceOrganisation(Base):
    __tablename__ = tb.info_source_organisation()

    info_source_orgn_uid = Column(Integer, Sequence(tb.info_source_organisation() + '_uid_seq', schema='public'),
                                  primary_key=True)
    orgn_name = Column(String)
    oranisation_url = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    info_source = relationship("InfoSource", back_populates='info_source_organisation')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_name(cls, session, orgn_name):
        return session.query(cls).filter_by(orgn_name=orgn_name)


class InfoSource(Base):
    __tablename__ = tb.info_source()

    info_source_uid = Column(Integer, Sequence(tb.info_source() + '_uid_seq', schema='public'),
                             primary_key=True)
    info_source_code = Column(String)
    info_source_orgn_uid = Column(Integer, ForeignKey(InfoSourceOrganisation.info_source_orgn_uid))
    info_source_name = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    info_source_organisation = relationship('InfoSourceOrganisation', back_populates='info_source')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_code(cls, session, info_source_code):
        return session.query(cls).filter_by(info_source_code=info_source_code)


class ExchangeOddsSeries(Base):
    __tablename__ = tb.exchange_odds_series()

    series_uid = Column(Integer, Sequence(tb.exchange_odds_series() + '_uid_seq', schema='public'),
                        primary_key=True)
    event_uid = Column(Integer, ForeignKey(Event.event_uid))
    market_uid = Column(Integer, ForeignKey(Market.market_uid))
    item_freq_type_code = Column(String)
    info_source_code = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    event = relationship('Market', back_populates='exchange_odds_series')
    market = relationship('Event', back_populates='exchange_odds_series')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_alternative_key(cls, session, event_uid, market_uid):
        return session.query(cls).filter_by(event_uid=event_uid, market_uid=market_uid)


class ExchangeOddsSeriesItem(Base):
    __tablename__ = tb.exchange_odds_series_item()

    series_item_uid = Column(Integer, Sequence(tb.exchange_odds_series_item() + '_uid_seq', schema='public'),
                             primary_key=True)
    series_uid = Column(Integer)
    runner_uid = Column(Integer)
    published_datetime = Column(DateTime)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_alternative_key(cls, session, event_uid, market_uid):
        return session.query(cls).filter_by(event_uid=event_uid, market_uid=market_uid)
