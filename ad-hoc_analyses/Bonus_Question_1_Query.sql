-- Bonus Question 1: How big a share of products that exist (products in the product portfolio) are sold each month? 

-- group by each year-month, 
-- what are the distinct products that are sold
-- how much of the sold products are in the product_portfolio table
-- calculate the fraction

WITH 
    -- first combine all of the three product_sales table into one, dropping the Zip, and Country columns because we don't care about them pretty much
    concatenated_product_sales AS (
        SELECT 
            Date, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_csv AS csv
        UNION ALL 
        SELECT
            Date, 
            OrderID,
            OrderLines
        FROM dbo.product_sales_json AS json
        UNION ALL 
        SELECT 
            Date, 
            OrderID, 
            OrderLines
        FROM dbo.product_sales_avro AS avro
    ), 
    -- open the json column into multiple other colums
    json_opened_product_sales AS(
        SELECT 
            cps.Date, 
            YEAR(cps.Date) AS Year, 
            MONTH(cps.Date) AS Month, 
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    ), 
    -- table storing the distinct product sold each year-month and whether that particular product exist or not
    distinct_productID_sold_each_month AS (
        SELECT DISTINCT
            Year, 
            Month, 
            ProductID, 
            CASE 
                WHEN ProductID IN (SELECT ProductID FROM dbo.product_portfolio) THEN 1
                ELSE 0
            END AS ProductExist -- creating a dummy column called 'ProductExist' which indicates whether a product exist or not in the product_portfolio table
        FROM json_opened_product_sales
    ), 
    -- table storing for each month, the total amount of distinct product that existed are sold and the total amount of distinct product that does not exist are sold
    monthly_existed_none_existed_product_sold AS (
        SELECT
            Year, 
            Month, 
            COUNT(CASE WHEN ProductExist = 1 THEN 1 END) AS TotalExistedProductSold, 
            COUNT(CASE WHEN ProductExist = 0 THEN 1 END) AS TotalNoneExistedProductSold
        FROM distinct_productID_sold_each_month
        GROUP BY Year, Month 
    )


SELECT 
    Year, 
    Month, 
    CAST(TotalExistedProductSold/(TotalExistedProductSold+TotalNoneExistedProductSold) AS FLOAT) AS ShareOfExistedProductSold 
FROM monthly_existed_none_existed_product_sold
ORDER BY Year, Month;

-- the resulting table indicates that for every single month the products that are sold all existed in the product_portfolio