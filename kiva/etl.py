import argparse
import asyncio
import json
import logging
import os
from json import JSONDecodeError

import aiohttp

from kiva import db
from kiva.models import Loan


APP_ID = os.environ.get('KIVA_APP_ID', '')
LIMIT = 100
URL = 'https://api.kivaws.org/v1/loans/{}.json'
if APP_ID:
    URL += f'?app_id={APP_ID}'
    RATE_LIMIT = 500
else:
    RATE_LIMIT = 60


logger = logging.getLogger('kiva.etl')


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
        logger.warning(text)
        loans = []
    return loans


async def etl_loans(start, end):
    print(f'Starting {start} => {end}')
    loan_ids = range(start, end + 1)
    loans = await retrieve_loans(loan_ids)
    loans = [Loan.transform(loan) for loan in loans]
    db.Session.add_all(loans)
    db.Session.commit()
    print(f'Completed {start} => {end}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('start', type=int)
    parser.add_argument('end', type=int)
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    group_size = int(RATE_LIMIT / 10)
    for start in range(args.start, args.end, group_size):
        end = min(start + group_size, args.end)
        loop.run_until_complete(asyncio.gather(*(
            etl_loans(offset, offset + LIMIT - 1)
            for offset in range(start, end, LIMIT)
        )))
