import json

from sqlalchemy import Column, DateTime, Integer, String

from kiva.client import Loan as LoanResource

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
    api_response = Column(String, nullable=False)

    @classmethod
    def create_from_id(cls, id_):
        loan = LoanResource.retrieve(id_)
        return cls._create_from_dict(loan)

    @classmethod
    def create_from_ids(cls, ids):
        loans = LoanResource.retrieve_ids(ids)
        return [cls._create_from_dict(loan) for loan in loans]

    @classmethod
    def _create_from_dict(cls, loan_dict):
        if not loan_dict:
            raise Exception('No data was returned from API')
        cols = set(cls.__table__.c.keys()) - {'country', 'api_response'}
        loan = cls(
            **{col: loan_dict.get(col) for col in cols},
            country=loan_dict['location']['country'],
            api_response=json.dumps(loan_dict)
        )
        return loan
