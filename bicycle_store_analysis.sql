/*
Analysis Project: Analyzing Bicycle Store Sales Data to Improve Sales Performance
**Problem:** Sales performance has shown noticeable declines in key periods. 
This analysis aims to uncover patterns that can inform data-driven marketing 
and pricing decisions.
**Solution:** Insights derived from aggregated and analyzed sales data.
**Outcome:** Identification of months with significant revenue and profit 
declines, highlighting areas for improvement.
**Proposed Solutions:** 
- Implement a marketing strategy that focuses on boosting sales before summer.
- Offer targeted discounts during the summer to counteract seasonal revenue declines.
**Next Steps:** 
- Expand the dataset to cover a broader time range to identify long-term patterns.
**Aggregation Level:** 
- The dataset lacks customer identifiers, insights are based on aggregated 
monthly sales behavior.
**Technologies Used:** 
- **Database:** MySQL 
- **Visualization:** Tableau 
  - A time-series graph will illustrate revenue and profit trends, 
	highlighting seasonal dips.
  - A tree map will help identify struggling product categories.

**Data Source:** 
- Kaggle: [Analyzing Customer Spending Habits]
(https://kaggle.com/datasets/thedevastator/analyzing-customer-spending-habits-to-improve-sales)
*/

-- Step 1: Create the sales_data table and load data for faster processing.
DROP TABLE IF EXISTS sales_data;
CREATE TABLE sales_data (
  order_id INT PRIMARY KEY, 
  order_date DATE,
  order_year INT,
  order_month VARCHAR(20),
  customer_age INT,
  customer_gender CHAR(1),
  country VARCHAR(100),
  state VARCHAR(100),
  product_category VARCHAR(100),
  sub_category VARCHAR(100),
  quantity FLOAT,
  unit_cost FLOAT,
  unit_price FLOAT,
  cost FLOAT,
  revenue FLOAT
);
-- Load contents into table
LOAD DATA INFILE 'C:/Path/To/File/sales_table.csv'
INTO TABLE sales_data
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS

-- Step 2: Verify table integrity by inspecting first 50 rows.
SELECT *
FROM sales_data
LIMIT 50;

-- Step 3: Identify missing values in key columns.
SELECT 
    COUNT(*) - COUNT(order_date) AS order_date_nulls,
    COUNT(*) - COUNT(order_year) AS order_year_nulls,
    COUNT(*) - COUNT(order_month) AS month_nulls,
    COUNT(*) - COUNT(customer_age) AS customer_age_nulls,
    COUNT(*) - COUNT(customer_gender) AS customer_gender_nulls,
    COUNT(*) - COUNT(country) AS country_nulls,
    COUNT(*) - COUNT(state) AS state_nulls,
    COUNT(*) - COUNT(product_category) AS product_category_nulls,
    COUNT(*) - COUNT(sub_category) AS sub_category_nulls,
    COUNT(*) - COUNT(quantity) AS quantity_nulls,
    COUNT(*) - COUNT(unit_cost) AS unit_cost_nulls,
    COUNT(*) - COUNT(unit_price) AS unit_price_nulls,
    COUNT(*) - COUNT(cost) AS cost_nulls,
    COUNT(*) - COUNT(revenue) AS revenue_nulls
FROM sales_data;

-- Step 4: Detect duplicate order IDs, if any.
SELECT 
	order_id, 
    COUNT(*) 
FROM sales_data 
GROUP BY order_id 
	HAVING COUNT(*) > 1;

-- Step 5: Analyze distribution of orders across countries and states.
SELECT 
	country, 
    COUNT(DISTINCT state) AS state_count,  
    COUNT(order_id) AS order_count
FROM sales_data 
GROUP BY country;

-- Step 6: Examine  distribution of orders across product categories.
SELECT 
	product_category, 
    COUNT(order_id) AS order_count
FROM sales_data 
GROUP BY product_category;

-- Step 7: Determine the time range covered in the dataset.
SELECT 
	MIN(order_date) AS first_order_date, 
	MAX(order_date) AS last_order_date, 
	CONCAT(
        TIMESTAMPDIFF(YEAR, MIN(order_date), MAX(order_date)), ' years, ',
        TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) % 12, ' months, ',
        DATEDIFF(MAX(order_date), MIN(order_date)) % 30, ' days'
    ) AS date_range
FROM sales_data;

-- Data range: 2015-01-01 until 2016-07-31
-- Step 8: Analyze the number of orders per month for each year.
SELECT 
	DATE_FORMAT(order_date, '%Y-%m') AS months,
    COUNT(CASE WHEN order_date BETWEEN '2015-01-01' AND '2015-12-31' THEN 1 END) AS entry_count_2015,
    COUNT(CASE WHEN order_date BETWEEN '2016-01-01' AND '2016-12-31' THEN 1 END) AS entry_count_2016
FROM sales_data
GROUP by months
ORDER BY months;

-- In first half of 2015 order amount is distincly smaller
-- Step 9: Analyze monthly trends in product categories using CTEs.
-- 'month_series' lists unique months.
-- 'category_series' lists unique product categories.
-- Final query joins both to track sales performance by category over time.
WITH month_series AS (
    SELECT DISTINCT DATE_FORMAT(order_date, '%Y-%m') AS months
    FROM sales_data
),
category_series AS (
    SELECT DISTINCT product_category
    FROM sales_data
)
SELECT 
    cs.product_category,
     ms.months,
    COUNT(sd.order_id) AS total_orders
FROM month_series ms
CROSS JOIN category_series cs
LEFT JOIN sales_data sd
    ON DATE_FORMAT(sd.order_date, '%Y-%m') = ms.months
    AND sd.product_category = cs.product_category
GROUP BY 
	cs.product_category,
    ms.months
ORDER BY 
    cs.product_category,
    ms.months;
    
-- Accessories and Clothing have no data for first half of 2015.
-- Step 10: Analyze monthly order distribution per country using CTEs.
-- 'month_series' lists unique months.
-- 'country_series' lists unique countries.
-- The final query joins both to track country-specific sales trends.
WITH month_series AS (
    SELECT DISTINCT DATE_FORMAT(order_date, '%Y-%m') AS months
    FROM sales_data
),
country_series AS (
    SELECT DISTINCT country
    FROM sales_data
)
SELECT 
    cs.country,
    ms.months,
    COUNT(sd.order_id) AS total_orders
FROM month_series ms
CROSS JOIN country_series cs
LEFT JOIN sales_data sd
    ON DATE_FORMAT(sd.order_date, '%Y-%m') = ms.months
    AND sd.country = cs.country
GROUP BY 
    cs.country,
    ms.months
ORDER BY 
    cs.country,
    ms.months;
    
-- Order values are confirmed to be smaller for first half of 2015. 
-- Analysis will focuse on month to month values
-- for time range 2015-07-01 until 2016-07-31. 

-- Step 11: Generate monthly summary of revenue, cost, and profit by country.
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') AS months, 
    country,
    SUM(revenue) AS total_revenue,
    SUM(cost) AS total_cost,
    SUM(revenue) - SUM(cost) AS total_profit,
    ROUND(AVG(revenue),2) AS revenue_per_order,
    ROUND(AVG(cost),2) AS cost_per_order,
    ROUND(AVG(revenue) - AVG(cost),2) AS profit_per_order
FROM sales_data
WHERE order_date BETWEEN '2015-07-01' AND '2016-07-31'
GROUP BY 
    months, country
ORDER BY 
    months, country;

-- Step 12: Analyze revenue and profit trends.
-- CTE 'profit_data' filters and precomputes monthly revenue and profit.
-- The main query applies window functions (LAG) to compare 
-- revenue, profit, and revenue per order month-over-month.
-- NULLIF() prevents division by zero.
-- Percentage changes highlight significant shifts in performance.
-- Rounding with ROUND() to 2 decimal places for readability.

WITH profit_data AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS months, 
        country,
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(revenue) - SUM(cost) AS total_profit,
        ROUND(AVG(revenue), 2) AS revenue_per_order,
        ROUND(AVG(cost), 2) AS cost_per_order,
        ROUND(AVG(revenue - cost), 2) AS profit_per_order
    FROM sales_data
    WHERE order_date BETWEEN '2015-07-01' AND '2016-07-31'
    GROUP BY 
        months, country
)

SELECT 
    country,
    months,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY country ORDER BY months) AS prev_month_total_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY country ORDER BY months)) / 
        NULLIF(LAG(total_revenue) OVER (PARTITION BY country ORDER BY months), 0) * 100,
        2) AS revenue_perc_change,
    total_profit,
    LAG(total_profit) OVER (PARTITION BY country ORDER BY months) AS prev_month_total_profit,
    ROUND(
        (total_profit - LAG(total_profit) OVER (PARTITION BY country ORDER BY months)) / 
        NULLIF(LAG(total_profit) OVER (PARTITION BY country ORDER BY months), 0) * 100,
        2) AS profit_perc_change,
    revenue_per_order,
    LAG(revenue_per_order) OVER (PARTITION BY country ORDER BY months) AS prev_month_revenue_per_order,
    ROUND(
        (revenue_per_order - LAG(revenue_per_order) OVER (PARTITION BY country ORDER BY months)) / 
        NULLIF(LAG(revenue_per_order) OVER (PARTITION BY country ORDER BY months), 0) * 100,
        2) AS revenue_per_order_perc_change,
    profit_per_order,
    LAG(profit_per_order) OVER (PARTITION BY country ORDER BY months) AS prev_month_profit_per_order,
    ROUND(
        (profit_per_order - LAG(profit_per_order) OVER (PARTITION BY country ORDER BY months)) / 
        NULLIF(LAG(profit_per_order) OVER (PARTITION BY country ORDER BY months), 0) * 100,
        2) AS profit_per_order_perc_change
FROM profit_data
ORDER BY 
    country, months;


-- Revenue and profit exhibit declining trend. 

-- Step 13: Profitability analysis of 'Accessories' in the United States.
-- This subcategory has the highest order count.
-- CTE 'profit_data' filters and precomputes monthly revenue and profit.
-- The main query applies window functions (LAG) to compare 
-- revenue, profit, and revenue per order month-over-month.

WITH profit_data AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS months, 
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(revenue) - SUM(cost) AS total_profit,
        ROUND(AVG(revenue), 2) AS revenue_per_order,
        ROUND(AVG(cost), 2) AS cost_per_order,
        ROUND(AVG(revenue - cost), 2) AS profit_per_order
    FROM sales_data
    WHERE order_date BETWEEN '2015-07-01' AND '2016-07-31' 
        AND country = 'United States'
        AND product_category = 'Accessories'
    GROUP BY 
        months
)

SELECT 
    months,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY months) AS prev_month_total_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY months)) / 
        NULLIF(LAG(total_revenue) OVER (ORDER BY months), 0) * 100,
        2) AS revenue_perc_change,
    total_profit,
    LAG(total_profit) OVER (ORDER BY months) AS prev_month_total_profit,
    ROUND(
        (total_profit - LAG(total_profit) OVER (ORDER BY months)) / 
        NULLIF(LAG(total_profit) OVER (ORDER BY months), 0) * 100,
        2) AS profit_perc_change,
    revenue_per_order,
    LAG(revenue_per_order) OVER (ORDER BY months) AS prev_month_revenue_per_order,
    ROUND(
        (revenue_per_order - LAG(revenue_per_order) OVER (ORDER BY months)) / 
        NULLIF(LAG(revenue_per_order) OVER (ORDER BY months), 0) * 100,
        2) AS revenue_per_order_perc_change,
    profit_per_order,
    LAG(profit_per_order) OVER (ORDER BY months) AS prev_month_profit_per_order,
    ROUND(
        (profit_per_order - LAG(profit_per_order) OVER (ORDER BY months)) / 
        NULLIF(LAG(profit_per_order) OVER (ORDER BY months), 0) * 100,
        2) AS profit_per_order_perc_change
FROM profit_data
ORDER BY 
    months;


-- Revenue and profit are mostly declining in 2016
-- 2015 show a few monthly declines, but no clear pattern. 
-- 2016 revenue and profit decline is predominant, particularly in June and July

-- Step 14: Breakdown of 'Accessories' profitability at subcategory level.
-- CTE 'profit_data' groups revenue and profit by subcategory and month.

WITH profit_data AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS months, 
        sub_category,
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(revenue) - SUM(cost) AS total_profit,
        ROUND(AVG(revenue), 2) AS revenue_per_order,
        ROUND(AVG(cost), 2) AS cost_per_order,
        ROUND(AVG(revenue - cost), 2) AS profit_per_order
    FROM sales_data
    WHERE order_date BETWEEN '2015-07-01' AND '2016-07-31' 
        AND country = 'United States'
        AND product_category = 'Accessories'
    GROUP BY 
        sub_category, months
)

SELECT 
    sub_category,
    months,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY sub_category ORDER BY months) AS prev_month_total_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY sub_category ORDER BY months)) / 
        NULLIF(LAG(total_revenue) OVER (PARTITION BY sub_category ORDER BY months), 0) * 100,
        2) AS revenue_perc_change,
    total_profit,
    LAG(total_profit) OVER (PARTITION BY sub_category ORDER BY months) AS prev_month_total_profit,
    ROUND(
        (total_profit - LAG(total_profit) OVER (PARTITION BY sub_category ORDER BY months)) / 
        NULLIF(LAG(total_profit) OVER (PARTITION BY sub_category ORDER BY months), 0) * 100,
        2) AS profit_perc_change,
    revenue_per_order,
    LAG(revenue_per_order) OVER (PARTITION BY sub_category ORDER BY months) AS prev_month_revenue_per_order,
    ROUND(
        (revenue_per_order - LAG(revenue_per_order) OVER (PARTITION BY sub_category ORDER BY months)) / 
        NULLIF(LAG(revenue_per_order) OVER (PARTITION BY sub_category ORDER BY months), 0) * 100,
        2) AS revenue_per_order_perc_change,
    profit_per_order,
    LAG(profit_per_order) OVER (PARTITION BY sub_category ORDER BY months) AS prev_month_profit_per_order,
    ROUND(
        (profit_per_order - LAG(profit_per_order) OVER (PARTITION BY sub_category ORDER BY months)) / 
        NULLIF(LAG(profit_per_order) OVER (PARTITION BY sub_category ORDER BY months), 0) * 100,
        2) AS profit_per_order_perc_change
FROM profit_data
ORDER BY 
    sub_category, months;



-- Some subcategories lack complete month-to-month data.
-- Helmets has best performance with general profit rise trend.
-- Revenu and profit exhibit various fluctuations. 
-- Clearest pattern of profit/revenue decline emerges from 
-- 2016 June and July - for most subcategories.

-- Step 15: Analyze  distribution of orders across customer age groups 
-- to understand the demographic profile of buyers.

SELECT 
    CASE
        WHEN customer_age BETWEEN 17 AND 24 THEN '17-24'
        WHEN customer_age BETWEEN 25 AND 34 THEN '25-34'
        WHEN customer_age BETWEEN 35 AND 44 THEN '35-44'
        WHEN customer_age BETWEEN 45 AND 54 THEN '45-54'
        WHEN customer_age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+' 
    END AS age_group,
    COUNT(order_id) AS order_count
FROM sales_data
WHERE order_date BETWEEN '2016-04-01' AND '2016-07-31'
    AND country = 'United States'
    AND product_category = 'Accessories'
GROUP BY age_group
ORDER BY age_group;


-- Step 16: Analyze revenue and profit trends across age groups and subcategories.
-- Focused on the period from April to July 2016.
-- profit_data' CTE segments revenue and profit by age group, subcategory, and month.

WITH profit_data AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS months, 
        sub_category,
        CASE
            WHEN customer_age BETWEEN 17 AND 24 THEN '17-24'
            WHEN customer_age BETWEEN 25 AND 34 THEN '25-34'
            WHEN customer_age BETWEEN 35 AND 44 THEN '35-44'
            WHEN customer_age BETWEEN 45 AND 54 THEN '45-54'
            WHEN customer_age BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+' 
        END AS age_group,
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(revenue) - SUM(cost) AS total_profit,
        ROUND(AVG(revenue), 2) AS revenue_per_order,
        ROUND(AVG(cost), 2) AS cost_per_order,
        ROUND(AVG(revenue - cost), 2) AS profit_per_order
    FROM sales_data
    WHERE order_date BETWEEN '2016-04-01' AND '2016-07-31' 
        AND country = 'United States'
        AND product_category = 'Accessories'
    GROUP BY 
        sub_category, months, age_group
)

SELECT 
    age_group,
    sub_category,
    months,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY age_group, sub_category ORDER BY months) AS prev_month_total_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY age_group, sub_category ORDER BY months)) / 
        NULLIF(LAG(total_revenue) OVER (PARTITION BY age_group, sub_category ORDER BY months), 0) * 100,
        2) AS revenue_perc_change,
    total_profit,
    LAG(total_profit) OVER (PARTITION BY age_group, sub_category ORDER BY months) AS prev_month_total_profit,
    ROUND(
        (total_profit - LAG(total_profit) OVER (PARTITION BY age_group, sub_category ORDER BY months)) / 
        NULLIF(LAG(total_profit) OVER (PARTITION BY age_group, sub_category ORDER BY months), 0) * 100,
        2) AS profit_perc_change,
    revenue_per_order,
    LAG(revenue_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months) AS prev_month_revenue_per_order,
    ROUND(
        (revenue_per_order - LAG(revenue_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months)) / 
        NULLIF(LAG(revenue_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months), 0) * 100,
        2) AS revenue_per_order_perc_change,
    profit_per_order,
    LAG(profit_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months) AS prev_month_profit_per_order,
    ROUND(
        (profit_per_order - LAG(profit_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months)) / 
        NULLIF(LAG(profit_per_order) OVER (PARTITION BY age_group, sub_category ORDER BY months), 0) * 100,
        2) AS profit_per_order_perc_change
FROM profit_data
ORDER BY 
    age_group, sub_category, months;


/* 
Conclusion:
- The analysis highlights a steady decline in revenue and profit, particularly in 2016.
- The 'Accessories' category in the U.S. shows a significant downturn in the summer months.
- Seasonal demand likely impacts sales—considering a discount campaign during summer could help offset losses.
- Further analysis with an extended dataset is needed to confirm whether this decline is a recurring yearly trend in June and July.
*/
