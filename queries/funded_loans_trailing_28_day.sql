WITH daily_funded_loans AS (
    SELECT
      CASE
      WHEN funded_date < '2006-04-19'
        THEN posted_date :: DATE
      ELSE funded_date :: DATE
      END                funded_on,
      count(*)           number_funded,
      sum(funded_amount) amount_funded
    FROM loans
    WHERE status = 'funded'
    GROUP BY funded_on
), days AS (
    SELECT generate_series(
               '2006-06-01',
               (SELECT max(funded_on)
                FROM daily_funded_loans),
               '1 day') :: DATE AS day

), daily_total AS (
    SELECT
      day,
      number_funded,
      amount_funded
    FROM days
      LEFT JOIN daily_funded_loans ON funded_on = day
)
SELECT
  day AS                                        "date",
  sum(number_funded)
  OVER
    (
    ORDER BY day
    ROWS BETWEEN 27 PRECEDING AND CURRENT ROW ) number_funded,
  sum(amount_funded)
  OVER
    (
    ORDER BY day
    ROWS BETWEEN 27 PRECEDING AND CURRENT ROW ) amount_funded
FROM daily_total
ORDER BY day
;