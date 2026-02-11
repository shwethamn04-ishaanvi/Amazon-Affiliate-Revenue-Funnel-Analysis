-- 1. Create the Database
CREATE DATABASE amazon_associates;
USE amazon_associates;

-- 2. Dimension Table: Products
CREATE TABLE amazon_products_catalog (
    product_asin VARCHAR(20) PRIMARY KEY,
    product_title VARCHAR(255),
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(10,2),
    commission_rate DECIMAL(5,4),
    inventory_status VARCHAR(50)
);

-- 3. Fact Table: User Traffic & Behavior
CREATE TABLE user_behavior_analytics (
    session_id VARCHAR(50),
    user_id VARCHAR(50),
    timestamp DATETIME,
    page_url VARCHAR(255),
    page_type VARCHAR(50),
    time_on_page_seconds INT,
    traffic_source VARCHAR(50),
    user_engagement_score DECIMAL(3,2),
    conversion_funnel_stage VARCHAR(50),
    PRIMARY KEY (session_id, timestamp)
);

-- 4. Fact Table: Affiliate Clicks
CREATE TABLE amazon_affiliate_clicks (
    click_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    session_id VARCHAR(50),
    timestamp DATETIME,
    product_asin VARCHAR(20),
    utm_source VARCHAR(50),
    utm_medium VARCHAR(50),
    utm_campaign VARCHAR(100)
);

-- 5. Fact Table: Conversions (The "Money" Table)
CREATE TABLE amazon_affiliate_conversions (
    conversion_id VARCHAR(50) PRIMARY KEY,
    click_id VARCHAR(50), -- Link to fact_clicks
    user_id VARCHAR(50),
    order_id VARCHAR(50),  -- Set as VARCHAR to handle 'C' prefixes
    timestamp DATETIME,
    product_asin VARCHAR(20),
    order_value DECIMAL(10,2),
    commission_earned DECIMAL(10,2),
    quantity_purchased INT,
    order_status VARCHAR(50),
    return_status VARCHAR(50)
);

-- 6. import data into table

LOAD DATA INFILE '/Users/shwetha/Downloads/cleaned_amazon_affiliate_clicks.csv'
INTO TABLE amazon_affiliate_conversions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(conversion_id, click_id, user_id, order_id, timestamp, product_asin, @dummy, @dummy, order_value, commission_rate, commission_earned, quantity_purchased, @dummy, @dummy, @dummy, @dummy, order_status, return_status, @dummy, @dummy);


-- SQL Analytics
-- view 1. The Master Conversion Funnel.This view  
-- calculates the "Drop-off" at every stage.It links user behavior to actual clicks and final sales.

CREATE OR REPLACE VIEW view_marketing_funnel AS
SELECT 
    t.traffic_source,
    COUNT(DISTINCT t.session_id) AS total_sessions,
    COUNT(DISTINCT c.click_id) AS total_clicks,
    COUNT(DISTINCT conv.conversion_id) AS total_conversions,
    -- Calculate CTR (Click-Through Rate)
    ROUND((COUNT(DISTINCT c.click_id) / COUNT(DISTINCT t.session_id)) * 100, 2) AS ctr_percentage,
    -- Calculate CVR (Conversion Rate)
    ROUND((COUNT(DISTINCT conv.conversion_id) / NULLIF(COUNT(DISTINCT c.click_id), 0)) * 100, 2) AS conversion_rate
FROM  user_behavior_analytics t
LEFT JOIN amazon_affiliate_clicks c ON t.session_id = c.session_id
LEFT JOIN amazon_affiliate_conversions conv ON c.click_id = conv.click_id
GROUP BY t.traffic_source;


SELECT * FROM view_marketing_funnel;

-- view 2.Product ROI & Commission Leakage
-- This view helps identify which products are "Leaky" (getting clicks but no sales) and which are your "Cash Cows."

CREATE OR REPLACE VIEW view_product_performance AS
SELECT 
    p.product_asin,
    p.product_title,
    p.category,
    COUNT(c.click_id) AS total_clicks,
    SUM(CASE WHEN conv.conversion_id IS NOT NULL THEN 1 ELSE 0 END) AS units_sold,
    SUM(conv.order_value) AS total_sales_value,
    SUM(conv.commission_earned) AS total_commission,
    -- Identify Leakage: Clicks with no sales
    CASE 
        WHEN COUNT(c.click_id) > 10 AND SUM(conv.order_value) IS NULL THEN 'High Leakage'
        WHEN SUM(conv.order_value) > 0 THEN 'Converting'
        ELSE 'Low Traffic'
    END AS performance_status
FROM amazon_products_catalog p
LEFT JOIN amazon_affiliate_clicks c ON p.product_asin = c.product_asin
LEFT JOIN amazon_affiliate_conversions conv ON c.product_asin = conv.product_asin
GROUP BY p.product_asin, p.product_title, p.category;

SELECT * FROM view_product_performance;

-- view 3. User Journey Timing
-- This helps you understand how long it takes for a user to actually buy something after clicking an affiliate link.


CREATE OR REPLACE VIEW view_conversion_time AS
SELECT 
    conv.conversion_id,
    conv.order_id,
    c.utm_source,
    c.timestamp AS click_time,
    conv.timestamp AS purchase_time,
    -- Calculate hours between click and purchase
    TIMESTAMPDIFF(HOUR, c.timestamp, conv.timestamp) AS hours_to_convert
FROM amazon_affiliate_conversions conv
JOIN amazon_affiliate_clicks c ON conv.click_id = c.click_id
WHERE conv.order_status = 'Delivered';

SELECT * FROM view_conversion_time;