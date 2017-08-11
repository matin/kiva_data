WITH funded_loans AS (
    SELECT
      id,
      CASE
      WHEN funded_date < '2006-04-19'
        THEN posted_date :: DATE
      ELSE funded_date :: DATE
      END funded_on
    FROM loans
    WHERE status = 'funded'
), lender_lended_on AS (
    SELECT
      fl.funded_on,
      ll.lender_id
    FROM funded_loans fl
      JOIN loan_lenders ll ON ll.loan_id = fl.id
), days AS (
    SELECT generate_series(
               (SELECT min(funded_on)
                FROM funded_loans),
               (SELECT max(funded_on)
                FROM funded_loans),
               '1 day') :: DATE AS day
)
SELECT
  day,
  (SELECT count(DISTINCT lender_id)
   FROM lender_lended_on
   WHERE funded_on BETWEEN day - INTERVAL '30 days' AND day)
FROM days
;
