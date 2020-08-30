from datetime import datetime

import pandas as pd
from sqlalchemy import Column, Integer, String, Numeric, Sequence, Boolean, DateTime, ForeignKey, or_
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

import utils.db.db_table_names as tb

Base = declarative_base()


class Country(Base):
    __tablename__ = tb.country()

    country_uid = Column(Integer, Sequence(tb.country() + '_country_uid_seq', schema='public'),
                         primary_key=True)
    country_name = Column(String)
    country_code = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    team = relationship('Team', back_populates='country')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_name(cls, session, country_name):
        return session.query(cls).filter_by(country_name=country_name).one()

    @classmethod
    def get_by_code(cls, session, country_code):
        return session.query(cls).filter_by(country_code=country_code).one()

    @classmethod
    def get_by_alternate_key(cls, session, country_name, country_code):
        return session.query(cls).filter_by(country_name=country_name,
                                            country_code=country_code).one()

    @classmethod
    def create_or_update(cls, session, country_name, country_code):
        try:
            country = cls.get_by_alternate_key(session, country_name=country_name,
                                               country_code=country_code)
            return country, True
        except Exception:
            session.add(cls(country_name=country_name, country_code=country_code))
            session.commit()
            country = cls.get_by_alternate_key(session, country_name=country_name,
                                               country_code=country_code)
            return country, False


class Team(Base):
    __tablename__ = tb.team()

    team_uid = Column(Integer, Sequence(tb.team() + '_team_uid_seq', schema='public'),
                      primary_key=True)
    team_name = Column(String)
    country_uid = Column(Integer, ForeignKey(Country.country_uid))
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    country = relationship('Country', back_populates='team')
    event_team_a = relationship('Event', back_populates='team_a', foreign_keys="[Event.team_a_uid]")
    event_team_b = relationship('Event', back_populates='team_b', foreign_keys="[Event.team_b_uid]")

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_team_name(cls, session, team_name):
        return session.query(cls).filter_by(team_name=team_name).one()

    @classmethod
    def get_by_alternate_key(cls, session, team_name, country):
        return session.query(cls).filter_by(team_name=team_name,
                                            country=country).one()

    @classmethod
    def create_or_update(cls, session, team_name, country):
        try:
            team = cls.get_by_alternate_key(session, team_name=team_name,
                                            country=country)
            return team, True
        except Exception:
            session.add(cls(team_name=team_name, country=country))
            session.commit()
            team = cls.get_by_alternate_key(session, team_name=team_name,
                                            country=country)
            return team, False


class Division(Base):
    __tablename__ = tb.division()

    division_uid = Column(Integer, Sequence(tb.division() + '_division_uid_seq', schema='public'),
                          primary_key=True)
    division_code = Column(String)
    division_name = Column(String)

    event = relationship('Event', back_populates='division')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_division_code(cls, session, division_code):
        return session.query(cls).filter_by(division_code=division_code).one()

    @classmethod
    def get_by_alternate_key(cls, session, division_code):
        return session.query(cls).filter_by(division_code=division_code).one()

    @classmethod
    def create_or_update(cls, session, division_code, division_name):
        try:
            division = cls.get_by_alternate_key(session, division_code=division_code)
            return division, True
        except Exception:
            session.add(cls(division_code=division_code,
                            division_name=division_name))
            session.commit()
            division = cls.get_by_alternate_key(session, division_code=division_code)
            return division, False


class Event(Base):
    __tablename__ = tb.event()

    event_uid = Column(Integer, Sequence(tb.event() + '_event_uid_seq', schema='public'),
                       primary_key=True)
    event_betfair_id = Column(String)
    team_a_uid = Column(Integer, ForeignKey(Team.team_uid), unique=True)
    team_b_uid = Column(Integer, ForeignKey(Team.team_uid), unique=True)
    division_code = Column(String, ForeignKey(Division.division_code))
    in_play_start = Column(DateTime)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    team_a = relationship("Team", back_populates="event_team_a", foreign_keys=[team_a_uid])
    team_b = relationship("Team", back_populates="event_team_b", foreign_keys=[team_b_uid])

    market = relationship('Market', back_populates='event')
    exchange_odds_series = relationship('ExchangeOddsSeries', back_populates='event')
    division = relationship('Division', back_populates='event')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_betfair_id(cls, session, betfair_id):
        return session.query(cls).filter_by(event_id=betfair_id).one()

    @classmethod
    def get_by_alternate_key(cls, session, event_betfair_id,
                             team_a, team_b):
        return session.query(cls).filter_by(event_betfair_id=event_betfair_id,
                                            team_a=team_a, team_b=team_b).one()

    @classmethod
    def get_by_teams(cls, session, teams):
        team_uids = [team.team_uid for team in teams]
        session.query(cls).filter(cls.team_a.in_(team_uids))

    @classmethod
    def create_or_update(cls, session, event_betfair_id,
                         team_a, team_b, in_play_start):
        try:
            event = cls.get_by_alternate_key(session, event_betfair_id=event_betfair_id,
                                             team_a=team_a, team_b=team_b)
            return event, True
        except Exception:
            session.add(cls(event_betfair_id=event_betfair_id,
                            team_a=team_a, team_b=team_b, in_play_start=in_play_start))
            session.commit()
            event = cls.get_by_alternate_key(session, event_betfair_id=event_betfair_id,
                                             team_a=team_a, team_b=team_b)
            return event, False


class MarketType(Base):
    __tablename__ = tb.market_type()

    market_type_code = Column(String, primary_key=True)
    market_type_desc = Column(String)

    market = relationship('Market', back_populates='market_type')

    @classmethod
    def get_by_code(cls, session, code):
        return session.query(cls).filter_by(market_type_code=code).one()

    @classmethod
    def get_by_alternate_key(cls, session, market_type_code, market_type_desc):
        return session.query(cls).filter_by(market_type_code=market_type_code,
                                            market_type_desc=market_type_desc).one()

    @classmethod
    def create_or_update(cls, session, market_type_code,
                         market_type_desc):
        try:
            market_type = cls.get_by_alternate_key(session, market_type_code=market_type_code,
                                                   market_type_desc=market_type_desc)
            return market_type, True
        except Exception:
            session.add(cls(market_type_code=market_type_code, market_type_desc=market_type_desc))
            session.commit()
            market_type = cls.get_by_alternate_key(session, market_type_code=market_type_code,
                                                   market_type_desc=market_type_desc)
            return market_type, False


class Market(Base):
    __tablename__ = tb.market()

    market_uid = Column(Integer, Sequence(tb.market() + '_market_uid_seq', schema='public'),
                        primary_key=True)
    event_uid = Column(Integer, ForeignKey(Event.event_uid))
    market_betfair_id = Column(String)
    market_type_code = Column(String, ForeignKey(MarketType.market_type_code))
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    event = relationship("Event", back_populates="market")
    exchange_odds_series = relationship('ExchangeOddsSeries', back_populates='market')
    market_type = relationship('MarketType', back_populates='market')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_betfair_id(cls, session, betfair_id):
        return session.query(cls).filter_by(event_id=betfair_id).one()

    @classmethod
    def get_by_alternate_key(cls, session, market_betfair_id, event, market_type_code):
        return session.query(cls).filter_by(market_betfair_id=market_betfair_id,
                                            event=event,
                                            market_type_code=market_type_code).one()

    @classmethod
    def create_or_update(cls, session, market_betfair_id, event, market_type_code):
        try:
            market = cls.get_by_alternate_key(session, market_betfair_id=market_betfair_id,
                                              event=event, market_type_code=market_type_code)
            return market, True
        except Exception:
            session.add(cls(market_betfair_id=market_betfair_id,
                            event=event,
                            market_type_code=market_type_code))
            session.commit()
            market = cls.get_by_alternate_key(session, market_betfair_id=market_betfair_id,
                                              event=event,
                                              market_type_code=market_type_code)
            return market, False


class Runner(Base):
    __tablename__ = tb.runner()

    runner_uid = Column(Integer, Sequence(tb.runner() + '_runner_uid_seq', schema='public'),
                        primary_key=True)
    runner_code = Column(String)
    runner_betfair_code = Column(String)

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_code(cls, session, runner_code):
        return session.query(cls).filter_by(runner_code=runner_code).one()

    @classmethod
    def get_by_alternate_key(cls, session, runner_code, runner_betfair_code):
        return session.query(cls).filter_by(runner_code=runner_code,
                                            runner_betfair_code=runner_betfair_code).one()

    @classmethod
    def create_or_update(cls, session, runner_code, runner_betfair_code):
        try:
            runner = cls.get_by_alternate_key(session, runner_code=runner_code,
                                              runner_betfair_code=runner_betfair_code)
            return runner, True
        except Exception:
            session.add(cls(runner_code=runner_code, runner_betfair_code=runner_betfair_code))
            session.commit()
            runner = cls.get_by_alternate_key(session, runner_code=runner_code,
                                              runner_betfair_code=runner_betfair_code)
            return runner, False


class InfoSourceOrganisation(Base):
    __tablename__ = tb.info_source_organisation()

    info_source_orgn_uid = Column(Integer, Sequence(tb.info_source_organisation() + '_info_source_orgn_uid_seq',
                                                    schema='public'),
                                  primary_key=True)
    organisation_name = Column(String)
    organisation_url = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    info_source = relationship("InfoSource", back_populates='info_source_organisation')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_name(cls, session, organisation_name):
        return session.query(cls).filter_by(organisation_name=organisation_name).one()

    @classmethod
    def get_by_alternate_key(cls, session, organisation_name, organisation_url):
        return session.query(cls).filter_by(organisation_name=organisation_name,
                                            organisation_url=organisation_url).one()

    @classmethod
    def create_or_update(cls, session, organisation_name, organisation_url):
        try:
            info_source_orgn = cls.get_by_alternate_key(session, organisation_name=organisation_name,
                                                        organisation_url=organisation_url)
            return info_source_orgn, True
        except Exception:
            session.add(cls(organisation_name=organisation_name, organisation_url=organisation_url))
            session.commit()
            info_source_orgn = cls.get_by_alternate_key(session, organisation_name=organisation_name,
                                                        organisation_url=organisation_url)
            return info_source_orgn, False


class InfoSource(Base):
    __tablename__ = tb.info_source()

    info_source_uid = Column(Integer, Sequence(tb.info_source() + '_info_source_uid_seq', schema='public'),
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
        return session.query(cls).filter_by(info_source_code=info_source_code).one()

    @classmethod
    def get_by_alternate_key(cls, session, info_source_code, info_source_organisation, info_source_name):
        return session.query(cls).filter_by(info_source_code=info_source_code,
                                            info_source_organisation=info_source_organisation,
                                            info_source_name=info_source_name).one()

    @classmethod
    def create_or_update(cls, session, info_source_code, info_source_organisation, info_source_name):
        try:
            info_source = cls.get_by_alternate_key(session, info_source_code=info_source_code,
                                                   info_source_organisation=info_source_organisation,
                                                   info_source_name=info_source_name)
            return info_source, True
        except Exception:
            session.add(cls(info_source_code=info_source_code,
                            info_source_organisation=info_source_organisation,
                            info_source_name=info_source_name))
            session.commit()
            info_source = cls.get_by_alternate_key(session, info_source_code=info_source_code,
                                                   info_source_organisation=info_source_organisation,
                                                   info_source_name=info_source_name)
            return info_source, False


class ExchangeOddsSeries(Base):
    __tablename__ = tb.exchange_odds_series()

    series_uid = Column(Integer, Sequence(tb.exchange_odds_series() + '_series_uid_seq', schema='public'),
                        primary_key=True)
    event_uid = Column(Integer, ForeignKey(Event.event_uid))
    market_uid = Column(Integer, ForeignKey(Market.market_uid))
    item_freq_type_code = Column(String)
    info_source_code = Column(String)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    event = relationship('Event', back_populates='exchange_odds_series')
    market = relationship('Market', back_populates='exchange_odds_series')

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_alternate_key(cls, session, event, market, item_freq_type_code,
                             info_source_code):
        return session.query(cls).filter_by(event=event, market=market,
                                            item_freq_type_code=item_freq_type_code,
                                            info_source_code=info_source_code).one()

    @classmethod
    def create_or_update(cls, session, event, market, item_freq_type_code,
                         info_source_code):
        try:
            series = cls.get_by_alternate_key(session, event=event, market=market,
                                              item_freq_type_code=item_freq_type_code,
                                              info_source_code=info_source_code)
            return series, True
        except Exception:
            session.add(cls(event=event, market=market,
                            item_freq_type_code=item_freq_type_code,
                            info_source_code=info_source_code))
            session.commit()
            series = cls.get_by_alternate_key(session, event=event, market=market,
                                              item_freq_type_code=item_freq_type_code,
                                              info_source_code=info_source_code)
            return series, False


class ExchangeOddsSeriesItem(Base):
    __tablename__ = tb.exchange_odds_series_item()

    series_item_uid = Column(Integer, Sequence(tb.exchange_odds_series_item() + '_uid_seq', schema='public'),
                             primary_key=True)
    series_uid = Column(Integer)
    runner_uid = Column(Integer)
    ltp = Column(Numeric)
    in_play = Column(Boolean)
    published_datetime = Column(DateTime)
    update_datetime = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())
    creation_datetime = Column(DateTime, default=datetime.utcnow())

    @classmethod
    def get_by_uid(cls, session, uid):
        return session.query(cls).get(uid)

    @classmethod
    def get_by_alternative_key(cls, session, event_uid, market_uid):
        return session.query(cls).filter_by(event_uid=event_uid, market_uid=market_uid).one()

    def insert_items_from_df(self, session, df):
        required_columns = ['runner_uid', 'published_datetime', 'ltp', 'in_play']
        if set(df.columns) != set(required_columns):
            raise ValueError("Dataframe must have columns [runner_uid, published_datetime, ltp]")
        df['series_uid'] = self.series_uid
        df['update_datetime'] = datetime.utcnow()
        df['creation_datetime'] = datetime.utcnow()

        df.to_sql(name=tb.exchange_odds_series_item(), schema='public', con=session.engine,
                  if_exists='append', method='multi')

    @classmethod
    def get_series_items_df(cls, session, runner_uid=None, market_uid=None, event_uid=None,
                            in_play=None):
        query = session.query(cls)
        if runner_uid:
            query = filter_by_list_or_str(cls, attr='runner_uid', query=query, val=runner_uid)
        if market_uid:
            query = filter_by_list_or_str(cls, attr='market_uid', query=query, val=market_uid)
        if event_uid:
            query = filter_by_list_or_str(cls, attr='event_uid', query=query, val=event_uid)
        if in_play is not None:
            query = query.filter(cls.in_play == in_play)

        return pd.read_sql(query.statement, query.session.bind)


class ItemFreqType(Base):
    __tablename__ = tb.item_freq_type()

    item_freq_type_code = Column(String, primary_key=True)
    item_freq_type_desc = Column(String)

    @classmethod
    def get_by_code(cls, session, code):
        return session.query(cls).filter_by(item_freq_type_code=code).one()

    @classmethod
    def get_by_alternate_key(cls, session, item_freq_type_code):
        return session.query(cls).filter_by(item_freq_type_code=item_freq_type_code).one()

    @classmethod
    def create_or_update(cls, session, item_freq_type_code, item_freq_type_desc):
        try:
            item_freq = cls.get_by_alternate_key(session, item_freq_type_code)
            return item_freq, True
        except Exception:
            session.add(cls(item_freq_type_code=item_freq_type_code, item_freq_type_desc=item_freq_type_desc))
            session.commit()
            item_freq = cls.get_by_alternate_key(session, item_freq_type_code=item_freq_type_code)
            return item_freq, False


def filter_by_list_or_str(cls, attr, query, val):
    if isinstance(val, list):
        query = query.filter(getattr(cls, attr).in_(val))
    elif isinstance(val, (str, int)):
        filter_condition = {attr: val}
        query = query.filter_by(**filter_condition)
    else:
        raise ValueError("must be string or list")

    return query
