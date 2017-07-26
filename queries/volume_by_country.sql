WITH country_loans AS (
  SELECT
    country,
    sum(funded_amount) funded_amount,
    avg(funded_amount) average,
    count(*) total
  FROM loans
  WHERE
    funded_date BETWEEN '2016-01-01' and '2016-12-31' AND
    status = 'funded'
  GROUP BY country
), total AS (
  SELECT
    sum(funded_amount) funded_amount,
    sum(total) total
  FROM country_loans
)
SELECT
  cl.country,
  cl.funded_amount::money,
  cl.total,
  cl.average::money,
  (cl.funded_amount / t.funded_amount) percent_of_funded_amount,
  (cl.total / t.total) percent_of_total
from country_loans cl, total t
order by total desc
;