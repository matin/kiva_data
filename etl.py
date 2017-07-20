from kiva import db
from kiva.models import Loan


LIMIT = 100


def etl_loans(start, end):
    if end - start > LIMIT:
        for offset in range(start, end, LIMIT):
            etl_loans(offset, offset + LIMIT)
    loans = Loan.create_from_ids(range(start, end))
    session = db.Session()
    session.add_all(loans)
    session.commit()
