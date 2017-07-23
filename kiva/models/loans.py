from sqlalchemy import Column, DateTime, Integer, JSON, String

from .base import Base


class Loan(Base):
    __tablename__ = 'loans'

    id = Column(Integer, primary_key=True, autoincrement=False)
    activity = Column(String)
    posted_date = Column(DateTime, index=True)
    funded_amount = Column(Integer, index=True)
    funded_date = Column(DateTime, index=True)
    lender_count = Column(Integer, index=True)
    loan_amount = Column(Integer, index=True)
    country = Column(String, index=True)
    status = Column(String, index=True)
    partner_id = Column(Integer, index=True)
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
