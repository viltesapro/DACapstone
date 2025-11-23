
-- Data validation --


CREATE OR REPLACE TABLE `final_project_dataset.customerdata` AS
SELECT
  `Customer ID` AS customer_id,
  `Purchase Date` AS purchase_date,
  `Product Category` AS product_category,
  `Product Price` AS price,
  `Quantity` AS quantity,
  `Total Purchase Amount` AS total_payed,
  `Payment Method` AS payment_method,
  `Customer Age` AS customer_age, -- dropped duplicate age column
  `Returns` AS returned,
  `Customer Name` AS customer_name,
  `Gender` AS gender,
  `Churn` AS churn
FROM `final_project_dataset.customerdata1`; 


-- Missing values --
SELECT
  *
FROM
  `final_project_dataset.customerdata`
WHERE
  customer_id IS NULL
  OR purchase_date IS NULL
  OR product_category IS NULL
  OR quantity IS NULL
  OR total_payed IS NULL
  OR payment_method IS NULL
  OR customer_age IS NULL
  OR customer_name IS NULL
  OR gender IS NULL
  OR churn IS NULL;

-- No data to display i.e. no missing values

-- Duplicates (only exact same rows are assumed duplicates) --
SELECT
  *,
  COUNT(*) AS duplicate_count
FROM
  `final_project_dataset.customerdata`
GROUP BY
  customer_id,
  customer_name,
  customer_age,
  gender,
  purchase_date,
  product_category,
  price,
  quantity,
  total_payed,
  payment_method,
  returned,
  churn
HAVING COUNT(*) > 1;

-- No data to display i.e. no duplicate rows

-- Checking negatives --
SELECT
  *
FROM
  `final_project_dataset.customerdata`
WHERE
  quantity <= 0
  OR price <= 0
  OR total_payed <= 0
  OR customer_age <= 0;

-- No data to display i.e. numerical variables comply with logic


-- Exploratory Data Analysis (results visualiazed in dashboard) --

-- Timeline
SELECT
  MIN(purchase_date) AS earliest,
  MAX(purchase_date) AS latest
FROM
  `final_project_dataset.customerdata`

-- Age range
SELECT
  MIN(customer_age) AS min_age,
  AVG(customer_age) AS avg_age,
  MAX(customer_age) AS max_age
FROM
  `final_project_dataset.customerdata`;

-- Gender counts
SELECT
  gender,
  COUNT(*) AS counts
FROM
  `final_project_dataset.customerdata`
GROUP BY
  gender;

-- Category counts
SELECT
  product_category,
  COUNT(*) AS counts
FROM
  `final_project_dataset.customerdata`
GROUP BY
  product_category;

-- Payment methods counts
SELECT
  payment_method,
  COUNT(*) AS counts
FROM
  `final_project_dataset.customerdata`
GROUP BY
  payment_method;

-- Returned counts
SELECT
  COUNTIF(returned IS NULL) AS null_count,
  COUNTIF(returned IS NOT NULL) AS non_null_count
FROM `final_project_dataset.customerdata`;

-- Customer count
SELECT
  COUNT(DISTINCT customer_id) AS no_of_customers
FROM
  `final_project_dataset.customerdata`;

-- Price range 
SELECT
  MIN(price) AS min_price,
  AVG(price) AS avg_price,
  MAX(price) AS max_price
FROM
  `final_project_dataset.customerdata`;

-- Quantity range
SELECT
  MIN(quantity) AS min_quantity,
  AVG(quantity) AS avg_quantity,
  MAX(customer_age) AS max_quantity
FROM
  `final_project_dataset.customerdata`;

-- Total payed range and sum
SELECT
  MIN(total_payed) AS min_total_payed,
  AVG(total_payed) AS avg_total_payed,
  MAX(total_payed) AS max_total_payed
  SUM(total_payed) AS revenue
FROM
  `final_project_dataset.customerdata`;


-- Recency, frequency and monetary scores. Customer segments. --

WITH
  base AS (
  SELECT
    customer_id,
    COUNT(*) AS frequency,
    SUM(total_payed) AS monetary,
    DATE_DIFF((
      SELECT
        DATE(MAX(purchase_date))
      FROM
        `final_project_dataset.customerdata`), DATE(MAX(purchase_date)), DAY) AS recency
  FROM
    `final_project_dataset.customerdata`
  GROUP BY
    customer_id ),
  quartiles AS (
  SELECT
    APPROX_QUANTILES(recency, 4) AS recency_quantiles,
    APPROX_QUANTILES(frequency, 4) AS frequency_quantiles,
    APPROX_QUANTILES(monetary, 4) AS monetary_quantiles
  FROM
    base),
  scores AS (
  SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    CASE
      WHEN recency <= quartiles.recency_quantiles[ OFFSET (1)] THEN 4
      WHEN recency <= quartiles.recency_quantiles[ OFFSET (2)] THEN 3
      WHEN recency <= quartiles.recency_quantiles[ OFFSET (3)] THEN 2
      ELSE 1
  END
    AS r_score,
    CASE
      WHEN frequency <= quartiles.frequency_quantiles[ OFFSET (1)] THEN 1
      WHEN frequency <= quartiles.frequency_quantiles[ OFFSET (2)] THEN 2
      WHEN frequency <= quartiles.frequency_quantiles[ OFFSET (3)] THEN 3
      ELSE 4
  END
    AS f_score,
    CASE
      WHEN monetary <= quartiles.monetary_quantiles[ OFFSET (1)] THEN 1
      WHEN monetary <= quartiles.monetary_quantiles[ OFFSET (2)] THEN 2
      WHEN monetary <= quartiles.monetary_quantiles[ OFFSET (3)] THEN 3
      ELSE 4
  END
    AS m_score,
  FROM
    base
  CROSS JOIN
    quartiles)
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CAST(r_score AS STRING) || CAST(f_score AS STRING) || CAST(m_score AS STRING) AS rfm_score,
  CASE
    WHEN r_score = 4 AND f_score = 4 AND m_score = 4 THEN 'Best Customers'
    WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
    WHEN r_score = 4 AND f_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
    WHEN r_score = 4 AND f_score = 1 THEN 'New Customers'
    WHEN r_score >= 3 AND f_score BETWEEN 2 AND 3 THEN 'Promising'
    WHEN r_score = 2 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3 THEN 'Needing Attention'
    WHEN r_score = 2 AND f_score <= 2 AND m_score <= 2 THEN 'About to Sleep'
    WHEN r_score <= 2 AND f_score >= 2 AND m_score >= 3 THEN 'Cannot Lose Them'
    WHEN r_score <= 2 AND f_score BETWEEN 2 AND 3 THEN 'At Risk'
    WHEN r_score = 1 AND f_score = 2 AND m_score <= 2 THEN 'Hibernating'
    WHEN r_score = 1 AND f_score = 1 AND m_score <= 2 THEN 'Lost'
    ELSE 'Others'
END
  AS segment
FROM
  scores;


-- Customer cohorts --

WITH
  first_purchase AS (
  SELECT
    customer_id,
    MIN(CAST(purchase_date AS DATE)) AS cohort_date
  FROM
    `final_project_dataset.customerdata`
  GROUP BY
    customer_id ),
  purchases_with_cohort AS (
  SELECT
    customerdata.customer_id,
    first_purchase.cohort_date,
    DATE_TRUNC(first_purchase.cohort_date, WEEK(MONDAY)) AS cohort_week,
    DATE_DIFF(DATE_TRUNC(CAST(customerdata.purchase_date AS DATE), WEEK(MONDAY)), DATE_TRUNC(first_purchase.cohort_date, WEEK(MONDAY)), WEEK) AS weeks_since_cohort
  FROM
    `final_project_dataset.customerdata` AS customerdata
  JOIN
    first_purchase
  ON
    customerdata.customer_id = first_purchase.customer_id ),
  retention AS (
  SELECT
    cohort_week,
    COUNT(DISTINCT customer_id) AS cohort_size,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 1, customer_id, NULL)) AS week_1,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 2, customer_id, NULL)) AS week_2,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 3, customer_id, NULL)) AS week_3,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 4, customer_id, NULL)) AS week_4,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 5, customer_id, NULL)) AS week_5,
    COUNT(DISTINCT
    IF
      (weeks_since_cohort >= 6, customer_id, NULL)) AS week_6
  FROM
    purchases_with_cohort
  GROUP BY
    cohort_week
  ORDER BY
    cohort_week )
SELECT
  cohort_week,
  cohort_size,
  ROUND(week_1 / cohort_size, 2) AS week_1_retention,
  ROUND(week_2 / cohort_size, 2) AS week_2_retention,
  ROUND(week_3 / cohort_size, 2) AS week_3_retention,
  ROUND(week_4 / cohort_size, 2) AS week_4_retention,
  ROUND(week_5 / cohort_size, 2) AS week_5_retention,
  ROUND(week_6 / cohort_size, 2) AS week_6_retention
FROM
  retention
QUALIFY
  DENSE_RANK() OVER (ORDER BY cohort_week) <= 15 # BETWEEN 55 and 69 / 106 AND 120 / 158 AND 172 
  -- chose to get each year's (2020-2023) first cohorts since there are marked differences 
ORDER BY
  cohort_week;



-- Data aggregated at customer level for churn analysis -- 

  SELECT
  customer_id,
  MAX(customer_age) AS age,
  ANY_VALUE(gender) AS gender,
  COUNT(DISTINCT purchase_date) AS num_purchases,
  COUNTIF(returned = 1.0) AS num_returns,
  SUM(total_payed) AS total_spent,
  AVG(total_payed) AS avg_order_value,
  AVG(quantity) AS avg_quantity,
  COUNT(DISTINCT product_category) AS num_unique_categories,
  COUNT(DISTINCT payment_method) AS num_payment_methods,
  MIN(purchase_date) AS first_purchase_date,
  DATE_DIFF(CURRENT_DATE(), DATE(MAX(purchase_date)), DAY) AS recency_days,
  DATE_DIFF(DATE(MAX(purchase_date)), DATE(MIN(purchase_date)), DAY) AS customer_lifetime_days,
  churn
FROM `final_project_dataset.customerdata`
GROUP BY customer_id, churn;
