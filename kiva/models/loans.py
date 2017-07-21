import json

from sqlalchemy import Column, DateTime, Integer, JSON, String

from .base import Base


class Loan(Base):
    __tablename__ = 'loans'

    id = Column(Integer, primary_key=True, autoincrement=False)
    activity = Column(String)
    posted_date = Column(DateTime)
    funded_amount = Column(Integer)
    funded_date = Column(DateTime)
    lender_count = Column(Integer)
    loan_amount = Column(Integer)
    country = Column(String)
    status = Column(String)
    api_response = Column(JSON, nullable=False)

    @classmethod
    def transform(cls, loan_dict):
        cols = set(cls.__table__.c.keys()) - {'country', 'api_response'}
        loan = cls(
            **{col: loan_dict.get(col) for col in cols},
            country=loan_dict['location']['country'],
            api_response=loan_dict
        )
        return loan
