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
), months AS (
    SELECT generate_series(
               '2005-05-01',
               (SELECT max(funded_on)
                FROM funded_loans),
               '1 month') :: DATE AS month
)
SELECT
  month AS Month,
  (SELECT count(DISTINCT lender_id)
   FROM lender_lended_on
   WHERE funded_on >= month AND funded_on < month + INTERVAL '1 month'
  )     AS "Active Lenders"
FROM months
;
