WITH country_loans AS (
    SELECT
      country,
      sum(funded_amount) funded_amount,
      count(*)           total,
      avg(funded_amount) average
    FROM loans
    WHERE
      funded_date BETWEEN '2016-01-01' AND '2016-12-31' AND
      status = 'funded'
    GROUP BY country
), total AS (
    SELECT
      sum(funded_amount) funded_amount,
      sum(total)         total
    FROM country_loans
)
SELECT
  cl.country,
  cl.funded_amount,
  cl.total,
  cl.average,
  (cl.funded_amount / t.funded_amount) percent_of_funded_amount,
  (cl.total / t.total)                 percent_of_total
FROM country_loans cl, total t
ORDER BY cl.total DESC
;