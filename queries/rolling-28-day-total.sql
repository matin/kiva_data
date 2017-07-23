copy (
WITH days AS (
  SELECT generate_series('2005-03-31'::date, '2017-07-21'::date, '1 day')::date AS day
), daily_funded_loans AS (
  SELECT
    CASE
      WHEN funded_date < '2006-04-19' THEN posted_date::date
          ELSE funded_date::date
    END funded_on,
    count(*) number_funded,
    sum(funded_amount) amount_funded
  FROM loans
  WHERE status = 'funded'
  GROUP BY funded_on
),
daily_total AS (
  SELECT
    day,
    number_funded,
    amount_funded
  FROM days
  LEFT JOIN daily_funded_loans ON funded_on = day
)
SELECT
  day,
  sum(number_funded) OVER
    (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) number_funded,
  sum(amount_funded) OVER
    (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)::money amount_funded
FROM daily_total
ORDER BY day
) to '/tmp/rolling-28-day-total.csv' with csv
;