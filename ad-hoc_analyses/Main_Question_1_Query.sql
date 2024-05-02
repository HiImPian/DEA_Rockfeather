-- Question 1: Which zip code has the highest sales per order for the latest month in the data set? 
-- 1. latest month, 
-- 2. group by zip code, 
-- 3. higehst SALES PER ORDER

-- because we only care about the zip code and not the countries for the first question, I will merge all the tables into one excluding the 'Country' column of the dbo.product_sales_csv table

-- creating common table expressions
WITH 
    -- extracting the latest month from the product_sales_csv table
    product_sales_csv_latest_month AS (
        SELECT
            MAX(MONTH(Date)) AS Latest_Month, 
            MAX(YEAR(Date)) AS Latest_Year
        FROM dbo.product_sales_csv
    ), 
    -- extractomg the latest month from the product_sales_json table
    product_sales_json_latest_month AS (
        SELECT 
            MAX(MONTH(Date)) AS Latest_Month, 
            MAX(YEAR(Date)) AS Latest_Year
        FROM dbo.product_sales_json
    ), 
    -- extracting the latest month from the product_sales_avro table
    product_sales_avro_latest_month AS (
        SELECT
            MAX(MONTH(Date)) AS Latest_Month, 
            MAX(YEAR(Date)) AS Latest_Year
        FROM dbo.product_sales_avro
    ), 
    -- filter for the observations in the product_sales_csv table at the latest month only
    filtered_product_sales_csv AS(
        SELECT 
            Date, 
            Zip, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_csv
        WHERE MONTH(Date) = (SELECT Latest_Month FROM product_sales_csv_latest_month)
            AND YEAR(Date) = (SELECT Latest_Year FROM product_sales_csv_latest_month) 
    ), 
    -- filter for the observations in the product_sales_json table at the latest month only
    filtered_product_sales_json AS(
        SELECT 
            Date, 
            Zip, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_json
        WHERE MONTH(Date) = (SELECT Latest_Month FROM product_sales_json_latest_month)
            AND YEAR(Date) = (SELECT Latest_Year FROM product_sales_json_latest_month)
    ), 
    -- filter for the observations in the product_sales_avro table at the latest month only
    filtered_product_sales_avro AS(
        SELECT 
            Date, 
            Zip, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_avro
        WHERE MONTH(Date) = (SELECT Latest_Month FROM product_sales_avro_latest_month)
            AND YEAR(Date) = (SELECT Latest_Year FROM product_sales_avro_latest_month)
    ), 
    -- concatenating the filtered tables into one using UNION ALL
    concatenated_product_sales AS(
        SELECT *
        FROM filtered_product_sales_csv
        UNION ALL
        SELECT *
        FROM filtered_product_sales_json
        UNION ALL
        SELECT *
        FROM filtered_product_sales_avro
    ), 
    -- opened the json column into multiple other columns of a row 
    json_opened_product_sales AS(
        SELECT 
            cps.Date, 
            cps.Zip, 
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    )

SELECT
    TOP (1) -- TOP (1) instead of LIMIT 1 at the end of the query because SQL Server does not recognize LIMIT but TOP instead
    Zip, 
    SUM(Revenue)/COUNT(OrderID) AS SalesPerOrder -- sum the revenue per order
FROM json_opened_product_sales
GROUP BY Zip
ORDER BY SalesPerOrder DESC; -- DESC to rank from highest SalesPerOrder to lowest

-- Ultimately, the Zip code with the highest amount of sales per order is 68005, with a total of sales per order of 1495,9875