-- Bonus Question 2: For each product (those that are sold more than two times) find the longest duration (measured in days) between two consecutive transactions. 
-- For example: if a product is sold on January 1st 2022, February 1st 2022 and October 1st 2022, then the longest duration between two transactions is the days between February 1st 2022 and on October 1st 2022

-- products that are sold more than two times
-- longest duration in days between two consecutive transactions 

-- create common table expression
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
            cps.OrderID, 
            CAST(JSON_VALUE(x.value, '$.ProductID') AS INT) AS ProductID, -- use CAST to conver the previous defined data type into the desired one
            CAST(JSON_VALUE(x.value, '$.Units') AS INT) AS Units, 
            CAST(JSON_VALUE(x.value, '$.Revenue') AS FLOAT) AS Revenue, 
            CAST(JSON_VALUE(x.value, '$.OrderLine') AS INT) AS OrderLine
        FROM concatenated_product_sales AS cps
        CROSS APPLY OPENJSON(OrderLines) AS x
    ), 
    -- a table storing information about for each product, how many times it has been sold
    number_of_times_a_product_is_sold AS (
        SELECT 
            ProductID, 
            COUNT(*) AS NumberOfTimesSold
        FROM json_opened_product_sales 
        GROUP BY ProductID
    ), 
    -- a table containing information about the product in which it has been sold more than twice
    relevant_products AS (
        SELECT 
            ProductID, 
            NumberOfTimesSold
        FROM number_of_times_a_product_is_sold
        WHERE NumberOfTimesSold > 2
    ), 
    relevant_products_sold_dates AS (
        SELECT 
            Date, 
            ProductID
        FROM json_opened_product_sales
        WHERE ProductID IN (SELECT ProductID FROM relevant_products)
    ), 
    relevant_products_sold_dates_lag AS (
        SELECT 
            ProductID,
            Date, 
            LAG(Date, 1) OVER(PARTITION BY ProductID ORDER BY Date ASC) AS PreviousDateSold
        FROM relevant_products_sold_dates
    ), 
    temp AS (
        SELECT 
            ProductID, 
            Date, 
            PreviousDateSold, 
            DATEDIFF(day, PreviousDateSold, Date) AS DaysAfterPreviousTransactionDate
        FROM relevant_products_sold_dates_lag
        WHERE PreviousDateSold IS NOT NULL
    )


SELECT
    ProductID, 
    MAX(DaysAfterPreviousTransactionDate) AS Longest_Duration
FROM temp 
GROUP BY ProductID;