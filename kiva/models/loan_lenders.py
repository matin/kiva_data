from sqlalchemy import Column, Integer, String

from .base import Base


class LoanLender(Base):
    __tablename__ = 'loan_lenders'

    loan_id = Column(Integer, primary_key=True, autoincrement=False)
    lender_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)

    @classmethod
    def transform(cls, loan_lenders, filename) -> list:
        loan_id = loan_lenders['id']
        lender_ids = loan_lenders['lender_ids'] or []
        return [
            LoanLender(loan_id=loan_id, lender_id=lender_id, filename=filename)
            for lender_id in lender_ids]
