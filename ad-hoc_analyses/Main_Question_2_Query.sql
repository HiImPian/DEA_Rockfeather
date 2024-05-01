-- Question 2: What is the market share of each country across all years? 
-- market share
-- each country
-- all years

-- create the 'Country' column for the product_sales_avro and product_sales_json and fill 'United States' for all rows
-- concatenate the updated product_sales_avro and product_sales_json together and also with the product_sales_csv
-- group the data per country and per year, 
-- calculate the market share

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
            cps.Zip, 
            cps.Country,
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine, 
            YEAR(Date) AS Year  -- a new year column because I would need it later on for the next table
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    ), 
    -- table storing the total annual sales across all years for the market (all countries combined)
    annual_total_product_sales AS (
        SELECT 
            Year, 
            SUM(Revenue) AS Market_Total_Revenue
        FROM json_opened_product_sales
        GROUP BY Year
    ), 
    -- table storing the total sales across all years for each country
    annual_total_product_sales_per_country AS (
        SELECT
            Year, 
            Country,
            SUM(Revenue) AS Country_Total_Revenue
        FROM json_opened_product_sales
        GROUP BY Year, Country
    )

SELECT 
    atpspc.Year, 
    atpspc.Country, 
    (atpspc.Country_Total_Revenue/atps.Market_Total_Revenue) * 100 AS Market_Share -- calculating the market share of each country in percentage terms
FROM annual_total_product_sales_per_country AS atpspc 
INNER JOIN annual_total_product_sales AS atps 
ON atpspc.Year = atps.Year
ORDER BY atpspc.Year, atpspc.Country;