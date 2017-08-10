from sqlalchemy import Column, Integer, String

from .base import Base


class LoanLender(Base):
    __tablename__ = 'loan_lenders'

    loan_id = Column(Integer, primary_key=True, autoincrement=False)
    lender_id = Column(String, primary_key=True)
