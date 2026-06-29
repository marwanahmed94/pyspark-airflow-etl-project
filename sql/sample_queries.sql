USE RetailDW;
GO

-- 1. Check total records in each table
SELECT COUNT(*) AS TotalRetailAnalyticsRows
FROM RetailAnalytics;

SELECT COUNT(*) AS TotalSalesByStateRows
FROM SalesByState;

SELECT COUNT(*) AS TotalSalesByCategoryRows
FROM SalesByCategory;

-- 2. Top 10 states by total sales
SELECT TOP 10
    customer_state,
    total_sales,
    total_freight
FROM SalesByState
ORDER BY total_sales DESC;

-- 3. Top 10 product categories by total sales
SELECT TOP 10
    product_category_name,
    total_sales,
    total_product_price
FROM SalesByCategory
ORDER BY total_sales DESC;

-- 4. Total sales and total freight
SELECT
    SUM(total_payment) AS TotalSales,
    SUM(freight_value) AS TotalFreight
FROM RetailAnalytics;

-- 5. Average delivery days
SELECT
    AVG(CAST(delivery_days AS FLOAT)) AS AverageDeliveryDays
FROM RetailAnalytics
WHERE delivery_days IS NOT NULL;

-- 6. Orders by year and month
SELECT
    purchase_year,
    purchase_month,
    COUNT(*) AS TotalOrders,
    SUM(total_payment) AS MonthlySales
FROM RetailAnalytics
GROUP BY purchase_year, purchase_month
ORDER BY purchase_year, purchase_month;

-- 7. Top 10 product categories by number of orders
SELECT TOP 10
    product_category_name,
    COUNT(*) AS TotalOrders
FROM RetailAnalytics
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY TotalOrders DESC;