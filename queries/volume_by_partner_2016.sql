WITH partner_loans AS (
    SELECT
      p.id               partner_id,
      p.name             partner_name,
      l.country,
      sum(funded_amount) funded_amount,
      count(*)           total,
      avg(funded_amount) average
    FROM loans l
      JOIN partners p ON p.id = l.partner_id
    WHERE
      l.funded_date BETWEEN '2016-01-01' AND '2016-12-31' AND
      l.status = 'funded'
    GROUP BY p.id, p.name, l.country
), total AS (
    SELECT
      sum(funded_amount) funded_amount,
      sum(total)         total
    FROM partner_loans
)
SELECT
  pl.partner_id,
  pl.partner_name,
  pl.country,
  pl.funded_amount,
  pl.total                             number_funded,
  pl.average,
  (pl.funded_amount / t.funded_amount) percent_of_funded_amount,
  (pl.total / t.total)                 percent_of_number_funded
FROM partner_loans pl, total t
ORDER BY pl.total DESC
;