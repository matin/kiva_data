from sqlalchemy import Column, Integer, String

from .base import Base


class LoanLender(Base):
    __tablename__ = 'loan_lenders'

    loan_id = Column(Integer, primary_key=True, autoincrement=False)
    lender_id = Column(String, primary_key=True)

    @classmethod
    def transform(cls, loan_lenders) -> list:
        lender_ids = set(loan_lenders['lender_ids'] or [])
        return [
            LoanLender(loan_id=loan_lenders['loan_id'], lender_id=lender_id)
            for lender_id in lender_ids]
