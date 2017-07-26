import argparse
import asyncio
import json
import logging
import os
from json import JSONDecodeError

import aiohttp
import requests

from kiva import db
from kiva.models import Loan, Partner


APP_ID = os.environ.get('KIVA_APP_ID', '')
LIMIT = 100
BASE_URL = 'https://api.kivaws.org/v1/{}.json'
if APP_ID:
    BASE_URL += f'?app_id={APP_ID}'
    RATE_LIMIT = 500
else:
    RATE_LIMIT = 60


logger = logging.getLogger('kiva.etl')


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def retrieve_loans(session, loan_ids):
    loan_ids = ','.join(str(loan_id) for loan_id in loan_ids)
    url = BASE_URL.format(f'loans/{loan_ids}')
    text = await fetch(session, url)
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


async def etl_loan_slice(start, end):
    print(f'Starting {start} => {end}')
    loan_ids = range(start, end + 1)
    async with aiohttp.ClientSession() as session:
        loans = await retrieve_loans(session, loan_ids)
    loans = [Loan.transform(loan) for loan in loans]
    db.Session.add_all(loans)
    db.Session.commit()
    print(f'Completed {start} => {end}')


def etl_loans(start, end):
    loop = asyncio.get_event_loop()
    group_size = int(RATE_LIMIT / 10)
    for start in range(start, end, group_size * LIMIT):
        end = min(start + group_size * LIMIT, args.end)
        loop.run_until_complete(asyncio.gather(*(
            etl_loan_slice(offset, offset + LIMIT - 1)
            for offset in range(start, end, LIMIT)
        )))


def etl_partners():
    url = BASE_URL.format('partners')
    resp = requests.get(url)
    partners = resp.json()['partners']
    partners = [Partner.transform(partner) for partner in partners]
    db.Session.add_all(partners)
    db.Session.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('resource')
    parser.add_argument('--start', type=int)
    parser.add_argument('--end', type=int)
    args = parser.parse_args()
    if args.resource == 'loans':
        etl_loans(args.start, args.end)
    elif args.resource == 'partners':
        etl_partners()
