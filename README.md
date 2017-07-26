# Kiva Data Analyzer

Scripts to download Kiva loan data from public API, load in a postgres db,
and analyze the data.

Uses `aiohttp` to speed up downloading. Also attempts to not exceed rate
limit.

### Install

`make install`

### Required environment variables

-   `DATABASE_URI`

### Load loans

`python kiva/etl.py loans`

### Loan partners

`python kiva/etl.py partners`