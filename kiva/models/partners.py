from sqlalchemy import Column, DateTime, Integer, JSON, String

from .base import Base


class Partner(Base):
    __tablename__ = 'partners'

    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String)
    status = Column(String, index=True)
    start_date = Column(DateTime)
    api_response = Column(JSON, nullable=False)

    @classmethod
    def transform(cls, partner_dict):
        cols = set(cls.__table__.c.keys()) - {'api_response'}
        partner = cls(
            **{col: partner_dict.get(col) for col in cols},
            api_response=partner_dict
        )
        return partner
