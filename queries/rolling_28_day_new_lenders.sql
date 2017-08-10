WITH days AS (
    SELECT generate_series('2005-03-31' :: DATE, '2017-07-21' :: DATE, '1 day') :: DATE AS day
), timestamped_lenders AS (
    SELECT
      ll.lender_id,
      CASE
      WHEN l.funded_date < '2006-04-19'
        THEN l.posted_date :: DATE
      ELSE l.funded_date :: DATE
      END funded_on
    FROM loan_lenders ll
      JOIN loans l ON l.id = ll.loan_id
    WHERE l.status = 'funded'
), timestamped_lenders_with_row_number AS (
    SELECT
      lender_id,
      funded_on,
      ROW_NUMBER()
      OVER (
        PARTITION BY lender_id
        ORDER BY funded_on ) AS row_num
    FROM timestamped_lenders
), lenders AS (
    SELECT
      lender_id,
      funded_on first_loan_on
    FROM timestamped_lenders_with_row_number
    WHERE row_num = 1
), daily_new_lenders AS (
    SELECT
      first_loan_on,
      count(*) new_lenders
    FROM lenders
    GROUP BY first_loan_on
), daily_total AS (
    SELECT
      day,
      new_lenders
    FROM days
      LEFT JOIN daily_new_lenders ON first_loan_on = day
)
SELECT
  day,
  sum(new_lenders)
  OVER (
    ORDER BY day
    ROWS BETWEEN 27 PRECEDING AND CURRENT ROW ) new_lenders
FROM daily_total
ORDER BY day
;