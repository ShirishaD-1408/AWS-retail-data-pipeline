
-- Step 5: Athena Queries for Retail Data Pipeline

-- 1️⃣ Create the external table pointing to processed Parquet data
CREATE EXTERNAL TABLE retail_transactions_parquet (
  `Order ID` string,
  `Customer ID` string,
  `Product` string,
  `Category` string,
  `Region` string,
  `Country` string,
  `Quantity` int,
  `Sales` double,
  Order_Date date
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://retail-processed-data-01/retail_transactions/';

-- 2️⃣ Repair partitions so Athena can see all year/month folders
MSCK REPAIR TABLE retail_transactions_parquet;

-- 3️⃣ Query: Top 10 products by total sales
SELECT Product, SUM(Sales) AS total_sales
FROM retail_transactions_parquet
GROUP BY Product
ORDER BY total_sales DESC
LIMIT 10;

-- 4️⃣ Query: Monthly sales trend
SELECT year, month, SUM(Sales) AS total_sales
FROM retail_transactions_parquet
GROUP BY year, month
ORDER BY year, month;
