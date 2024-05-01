-- Bonus Question 3: For each month, which country has the higehst year-to-date (YTD) growth since previous year's YTD
-- The growth calculation is: ([Revenue YTD] - [Revenue YTD previous year])/[Revenue YTD previous year]

-- each month 
-- which country
-- highest YTD growth since previous year's YTD

-- creating common table expressions
WITH 
    -- update the dbo.product_sales_json table by adding an additional column called 'Country' where it is filled with United States
    updated_product_sales_json AS (
        SELECT 
            Date,
            CAST('United Sates' AS VARCHAR(MAX)) AS Country, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_json
    ), 
    -- update the dbo.product_sales_avro table by adding an additional column called 'Country' where it is filled with United States
    updated_product_sales_avro  AS (
        SELECT 
            Date,
            CAST('United Sates' AS VARCHAR(MAX)) AS Country, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_avro
    ), 
    -- concatenating the filtered tables into one using UNION ALL
    concatenated_product_sales AS(
        SELECT 
            Date, 
            Country, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_csv
        UNION ALL
        SELECT *
        FROM updated_product_sales_json
        UNION ALL
        SELECT *
        FROM updated_product_sales_avro
    ), 
    -- opened the json column into multiple other columns of a row 
    json_opened_product_sales AS(
        SELECT 
            cps.Date, 
            YEAR(cps.Date) AS Year, 
            MONTH(cps.Date) AS Month,
            cps.Country,
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    ), 
    country_total_monthly_revenue AS (
        SELECT
            Year, 
            Month, 
            Country, 
            SUM(Revenue) AS Country_Monthly_Total_Revenue
        FROM json_opened_product_sales 
        GROUP BY Year, Month, Country
    ), 
    unsorted_country_monthly_YTD_revenue AS (
        SELECT
            Year, 
            Month, 
            Country, 
            Country_Monthly_Total_Revenue, 
            SUM(Country_Monthly_Total_Revenue) OVER(PARTITION BY Year, Country ORDER BY Year, Month) AS YTD_Revenue
        FROM country_total_monthly_revenue
    ), 
    unsorted_country_monthly_YTD_PY_YTD_Revenue AS (
        SELECT
            Year, 
            Month, 
            Country, 
            YTD_Revenue, 
            LAG(YTD_Revenue, 12) OVER(PARTITION BY Country ORDER BY Year, Month) AS PY_YTD_Revenue
        FROM unsorted_country_monthly_YTD_revenue
    ), 
    unsorted_country_monthly_YTD_growth_since_PY AS (
        SELECT
            Year, 
            Month, 
            Country, 
            (YTD_Revenue - PY_YTD_Revenue)/PY_YTD_Revenue AS YTD_Growth_Since_PY
        FROM unsorted_country_monthly_YTD_PY_YTD_Revenue
        WHERE PY_YTD_Revenue IS NOT NULL
    )

-- because there is no previous year for 2016, the final results won't have the month in 2016

-- country_monthly_YTD_revenue
/* 
SELECT 
    Year, 
    Month, 
    Country, 
    YTD_Revenue
FROM unsorted_country_monthly_YTD_revenue
ORDER BY Country, Year, Month; 
*/

SELECT
    Year, 
    Month, 
    Country, 
    MAX(YTD_Growth_Since_PY) OVER (ORDER BY Year, Month) AS Highest_YTD_Growth_Since_PY
FROM unsorted_country_monthly_YTD_growth_since_PY
ORDER BY Country, Year, Month;