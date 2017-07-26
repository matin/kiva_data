# Kiva Data Analyzer

Scripts to download Kiva loan data from public API, load in a postgres db,
and analyze the data.

### Install

`make install`

### Required environment variables

-   `DATABASE_URI`

### Load loans

`python kiva/etl.py loans`

### Loan partners

`python kiva/etl.py partners`