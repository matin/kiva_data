import json

from sqlalchemy import Column, DateTime, Integer, String

from kiva.client import Loan as LoanResource

from .base import Base


class Loan(Base):
    __tablename__ = 'loans'

    id = Column(Integer, primary_key=True, autoincrement=False)
    activity = Column(String)
    funded_amount = Column(Integer)
    funded_date = Column(DateTime)
    lender_count = Column(Integer)
    loan_amount = Column(Integer)
    country = Column(String)
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
        loan = cls(
            id=loan_dict['id'],
            activity=loan_dict['activity'],
            funded_amount=loan_dict['funded_amount'],
            funded_date=loan_dict['funded_date'],
            lender_count=loan_dict['lender_count'],
            loan_amount=loan_dict['loan_amount'],
            country=loan_dict['location']['country'],
            api_response=json.dumps(loan_dict)
        )
        return loan
