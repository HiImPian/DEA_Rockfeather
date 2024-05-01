-- Question 3: For each month, which country generated the highest revenue?
-- show also which product that contributed mostly to the revenue for that parciular country and month

-- each month
-- which country
-- highest revenue

-- product with the highest revenue for that particular country and month

-- creating common table expressions
WITH 
    -- update the dbo.product_sales_json table by adding an additional column called 'Country' where it is filled with United States
    updated_product_sales_json AS (
        SELECT 
            Date,
            Zip, 
            CAST('United Sates' AS VARCHAR(MAX)) AS Country, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_json
    ), 
    -- update the dbo.product_sales_avro table by adding an additional column called 'Country' where it is filled with United States
    updated_product_sales_avro  AS (
        SELECT 
            Date,
            Zip, 
            CAST('United Sates' AS VARCHAR(MAX)) AS Country, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_avro
    ), 
    -- concatenating the filtered tables into one using UNION ALL
    concatenated_product_sales AS(
        SELECT *
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
            YEAR(Date) AS Year, 
            MONTH(Date) AS Month,
            cps.Zip, 
            cps.Country,
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    ), 
    -- table containing the monthly total revenue per country
    monthly_total_revenue_per_country AS (
        SELECT
            Year, 
            Month, 
            Country, 
            SUM(Revenue) AS Country_Total_Revenue 
        FROM json_opened_product_sales 
        GROUP BY Year, Month, Country
    ),
    -- table containing the revenue of the highest revenue generating country of each year-month
    monthly_highest_country_total_revenue AS (
        SELECT
            Year, 
            Month, 
            MAX(Country_Total_Revenue) AS Highest_Country_Total_Revenue
        FROM monthly_total_revenue_per_country
        GROUP BY Year, Month
    ), 
    -- table containing the country that generate the highest revenue of that year-month
    highest_revenue_generating_country_per_month AS (
        SELECT
        mtrpc.Year, 
        mtrpc.Month, 
        mtrpc.Country AS Highest_Revenue_Country, 
        mtrpc.Country_Total_Revenue
    FROM monthly_total_revenue_per_country AS mtrpc 
    INNER JOIN monthly_highest_country_total_revenue AS mhctr 
    ON 
        mtrpc.Year = mhctr.Year AND
        mtrpc.Month = mhctr.Month AND 
        mtrpc.Country_Total_Revenue = mhctr.Highest_Country_Total_Revenue 
    ),
    -- table containing the monthly total revenue per product per highest revenue generating country per month
    monthly_total_product_revenue_per_country AS (
        SELECT
            Year, 
            Month, 
            Country, 
            ProductID, 
            SUM(Revenue) AS Total_Product_Revenue
        FROM json_opened_product_sales
        WHERE Country IN (SELECT Highest_Revenue_Country FROM highest_revenue_generating_country_per_month)
        GROUP BY Year, Month, Country, ProductID
    ), 
    -- table containing the product revenue of the highest revenue generating product per country and of each year-month
    monthly_most_important_product_per_country AS (
        SELECT
            Year, 
            Month, 
            Country, 
            MAX(Total_Product_Revenue) AS Highest_Product_Revenue
        FROM monthly_total_product_revenue_per_country
        GROUP BY Year, Month, Country
    ), 
    -- table containing the product that generate the highest revenue for that country and of each year-month
    highest_revenue_generating_product_per_month_country AS (
    SELECT
        mtprpc.Year, 
        mtprpc.Month,
        mtprpc.Country, 
        mtprpc.ProductID AS Most_Important_ProductID, 
        mtprpc.Total_Product_Revenue
    FROM monthly_total_product_revenue_per_country AS mtprpc 
    INNER JOIN monthly_most_important_product_per_country AS mmippc
    ON
        mtprpc.Year = mmippc.Year AND
        mtprpc.Month = mmippc.Month AND 
        mtprpc.Total_Product_Revenue = mmippc.Highest_Product_Revenue
    )


SELECT
    hrgc.Year, 
    hrgc.Month, 
    hrgc.Highest_Revenue_Country, 
    hrgp.Most_Important_ProductID
FROM highest_revenue_generating_country_per_month AS hrgc
INNER JOIN highest_revenue_generating_product_per_month_country AS hrgp
ON 
    hrgc.Year = hrgp.Year AND 
    hrgc.Month = hrgp.Month AND 
    hrgc.Highest_Revenue_Country = hrgp.Country;
