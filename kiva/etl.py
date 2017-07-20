import argparse
import asyncio
import json
from json import JSONDecodeError

import aiohttp

from kiva import db
from kiva.models import Loan


LIMIT = 100
URL = 'https://api.kivaws.org/v1/loans/{}.json'


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def retrieve_loans(loan_ids):
    loan_ids = ','.join(str(loan_id) for loan_id in loan_ids)
    async with aiohttp.ClientSession() as session:
        text = await fetch(session, URL.format(loan_ids))
    try:
        resp_dict = json.loads(text)
    except JSONDecodeError:
        text = text.replace('\\', '')
        resp_dict = json.loads(text)
    try:
        loans = resp_dict['loans']
    except KeyError:
        loans = []
    return loans


async def etl_loans(start, end):
    loan_ids = range(start, end + 1)
    loans = await retrieve_loans(loan_ids)
    loans = [Loan.transform(loan) for loan in loans]
    db.Session.add_all(loans)
    db.Session.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start', type=int)
    parser.add_argument('end', type=int)
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*(
        etl_loans(offset, offset + LIMIT - 1)
        for offset in range(args.start, args.end, LIMIT)
    )))
