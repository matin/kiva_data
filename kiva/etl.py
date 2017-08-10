import argparse
import asyncio
import json
import logging
import os
from json import JSONDecodeError

import aiohttp
import requests
from sqlalchemy.exc import IntegrityError

from kiva import db
from kiva.models import Loan, LoanLender, Partner


APP_ID = os.environ.get('KIVA_APP_ID', '')
LIMIT = 100
BASE_URL = 'https://api.kivaws.org/v1/{}.json'
if APP_ID:
    BASE_URL += f'?app_id={APP_ID}'
    RATE_LIMIT = 500
else:
    RATE_LIMIT = 60
RATE_LIMIT_FACTOR = 10


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


def etl_loans(start=None, end=None):
    if start is None:
        try:
            last_loan = Loan.query.order_by(Loan.id.desc())[0]
        except IndexError:
            start = 0
        else:
            start = last_loan.id + 1
    if end is None:
        resp = requests.get(BASE_URL.format('loans/newest'))
        end = resp.json()['loans'][0]['id']
    loop = asyncio.get_event_loop()
    slice_size = int(RATE_LIMIT / RATE_LIMIT_FACTOR)
    for slice_start in range(start, end, slice_size * LIMIT):
        slice_end = min(slice_start + slice_size * LIMIT, end)
        loop.run_until_complete(asyncio.gather(*(
            etl_loan_slice(offset, offset + LIMIT - 1)
            for offset in range(slice_start, slice_end, LIMIT)
        )))


def etl_loan_lenders(dirpath, filename):
    print(f'working on {filename} ...')
    with open(os.path.join(dirpath, filename)) as f:
        loans_lenders = json.load(f)['loans_lenders']
    for loan_lenders in loans_lenders:
        loan_id = loans_lenders['id']
        lender_ids = loan_lenders['lender_ids']
        if lender_ids:
            db.Session.add_all(
                LoanLender(loan_id=loan_id, lender_id=lender_id,
                           filename=filename)
                for lender_id in lender_ids)
        try:
            db.Session.commit()
        except IntegrityError:
            logger.error(f'duplicate entries with loan id {loan_id}')
            db.Session.rollback()


def etl_loans_lenders(dirpath):
    filenames = set(os.listdir(dirpath))
    processed_files = db.engine.execute(
        'SELECT distinct(filename) from loan_lenders')
    filenames -= {filename[0] for filename in processed_files}
    for filename in filenames:
        etl_loan_lenders(dirpath, filename)


def etl_partners():
    url = BASE_URL.format('partners')
    resp = requests.get(url)
    partners = resp.json()['partners']
    partners = [Partner.transform(partner) for partner in partners]
    db.Session.add_all(partners)
    db.Session.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('resource')
    parser.add_argument('--start', type=int)
    parser.add_argument('--end', type=int)
    parser.add_argument('--dirpath')
    args = parser.parse_args()
    if args.resource == 'loans':
        etl_loans(args.start, args.end)
    if args.resource == 'loans_lenders':
        etl_loans_lenders(args.dirpath)
    elif args.resource == 'partners':
        etl_partners()


if __name__ == '__main__':
    main()
