-- Banking Analytics SQL Queries for EFT Banking Pipeline

-- ============================================
-- Query 1: Top 5 banks by transaction volume in the last 7 days
-- ============================================

SELECT 
    bank_id,
    SUM(total_volume) as total_transaction_volume,
    SUM(transaction_count) as total_transactions,
    AVG(avg_transaction_value) as average_transaction_value,
    COUNT(DISTINCT transaction_date) as active_days,
    MAX(transaction_date) as latest_transaction_date
FROM daily_bank_aggregates
WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    AND transaction_date <= CURDATE()
GROUP BY bank_id
ORDER BY total_transaction_volume DESC
LIMIT 5;

-- ============================================
-- Enhanced version with additional metrics
-- ============================================

WITH bank_weekly_stats AS (
    SELECT 
        bank_id,
        SUM(total_volume) as total_volume,
        SUM(transaction_count) as total_transactions,
        AVG(avg_transaction_value) as avg_transaction_value,
        STDDEV(total_volume) as volume_volatility,
        COUNT(DISTINCT transaction_date) as active_days,
        MIN(transaction_date) as first_transaction_date,
        MAX(transaction_date) as last_transaction_date
    FROM daily_bank_aggregates
    WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        AND transaction_date <= CURDATE()
    GROUP BY bank_id
),
bank_rankings AS (
    SELECT *,
        RANK() OVER (ORDER BY total_volume DESC) as volume_rank,
        RANK() OVER (ORDER BY total_transactions DESC) as transaction_rank,
        ROUND(total_volume / NULLIF(total_transactions, 0), 2) as calculated_avg_value
    FROM bank_weekly_stats
)
SELECT 
    volume_rank,
    bank_id,
    FORMAT(total_volume, 2) as formatted_total_volume,
    total_transactions,
    FORMAT(avg_transaction_value, 2) as avg_transaction_value,
    active_days,
    CASE 
        WHEN active_days = 7 THEN 'Consistent'
        WHEN active_days >= 5 THEN 'Regular'
        ELSE 'Irregular'
    END as activity_pattern,
    FORMAT(volume_volatility, 2) as volume_volatility
FROM bank_rankings
WHERE volume_rank <= 5
ORDER BY volume_rank;

-- ============================================
-- Query 2: Average transaction value per customer for a given month
-- ============================================

-- For a specific month (example: September 2025)
SELECT 
    bank_id,
    YEAR(transaction_date) as year,
    MONTH(transaction_date) as month,
    MONTHNAME(transaction_date) as month_name,
    SUM(total_volume) / SUM(unique_customers) as avg_transaction_value_per_customer,
    SUM(total_volume) as total_monthly_volume,
    SUM(unique_customers) as total_unique_customers,
    AVG(avg_transaction_value) as avg_individual_transaction_value,
    COUNT(transaction_date) as active_days_in_month
FROM daily_bank_aggregates
WHERE YEAR(transaction_date) = 2025 
    AND MONTH(transaction_date) = 9  -- September 2025
GROUP BY bank_id, YEAR(transaction_date), MONTH(transaction_date)
ORDER BY avg_transaction_value_per_customer DESC;

-- ============================================
-- Enhanced monthly analysis with trends
-- ============================================

WITH monthly_customer_metrics AS (
    SELECT 
        bank_id,
        DATE_FORMAT(transaction_date, '%Y-%m') as year_month,
        SUM(total_volume) as monthly_volume,
        SUM(unique_customers) as total_customers,
        SUM(transaction_count) as total_transactions,
        AVG(avg_transaction_value) as avg_individual_transaction,
        COUNT(DISTINCT transaction_date) as active_days,
        MAX(data_quality_score) as best_quality_score
    FROM daily_bank_aggregates
    WHERE transaction_date >= '2025-09-01' 
        AND transaction_date < '2025-10-01'
    GROUP BY bank_id, DATE_FORMAT(transaction_date, '%Y-%m')
),
customer_analytics AS (
    SELECT *,
        ROUND(monthly_volume / NULLIF(total_customers, 0), 2) as avg_value_per_customer,
        ROUND(total_transactions / NULLIF(total_customers, 0), 2) as avg_transactions_per_customer,
        ROUND(monthly_volume / NULLIF(active_days, 0), 2) as avg_daily_volume,
        ROUND((total_customers / NULLIF(active_days, 0)), 2) as avg_customers_per_day
    FROM monthly_customer_metrics
)
SELECT 
    bank_id,
    year_month,
    FORMAT(avg_value_per_customer, 2) as avg_transaction_value_per_customer,
    FORMAT(monthly_volume, 2) as total_monthly_volume,
    total_customers,
    avg_transactions_per_customer,
    FORMAT(avg_individual_transaction, 2) as avg_individual_transaction_value,
    active_days,
    FORMAT(avg_daily_volume, 2) as avg_daily_volume,
    ROUND(best_quality_score, 1) as data_quality_percentage,
    CASE 
        WHEN avg_value_per_customer >= 10000 THEN 'High Value'
        WHEN avg_value_per_customer >= 5000 THEN 'Medium Value'
        ELSE 'Standard Value'
    END as customer_segment
FROM customer_analytics
ORDER BY avg_value_per_customer DESC;

-- ============================================
-- Additional Analytics Queries for Dashboard Support
-- ============================================

-- Query 3: Daily volume trends with anomaly detection
WITH daily_stats AS (
    SELECT 
        transaction_date,
        SUM(total_volume) as daily_total_volume,
        COUNT(DISTINCT bank_id) as active_banks,
        AVG(avg_transaction_value) as system_avg_transaction
    FROM daily_bank_aggregates
    WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY transaction_date
),
volume_stats AS (
    SELECT *,
        AVG(daily_total_volume) OVER (ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7day,
        STDDEV(daily_total_volume) OVER (ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_stddev_7day
    FROM daily_stats
)
SELECT 
    transaction_date,
    FORMAT(daily_total_volume, 2) as daily_volume,
    FORMAT(moving_avg_7day, 2) as seven_day_avg,
    active_banks,
    FORMAT(system_avg_transaction, 2) as avg_transaction_value,
    CASE 
        WHEN daily_total_volume > (moving_avg_7day + 2 * moving_stddev_7day) THEN 'High Anomaly'
        WHEN daily_total_volume < (moving_avg_7day - 2 * moving_stddev_7day) THEN 'Low Anomaly'
        WHEN daily_total_volume > (moving_avg_7day + moving_stddev_7day) THEN 'Above Normal'
        WHEN daily_total_volume < (moving_avg_7day - moving_stddev_7day) THEN 'Below Normal'
        ELSE 'Normal'
    END as volume_status
FROM volume_stats
WHERE moving_avg_7day IS NOT NULL
ORDER BY transaction_date DESC;

-- Query 4: Bank performance comparison
SELECT 
    bank_id,
    COUNT(DISTINCT transaction_date) as total_active_days,
    FORMAT(SUM(total_volume), 2) as total_lifetime_volume,
    FORMAT(AVG(total_volume), 2) as avg_daily_volume,
    SUM(transaction_count) as total_transactions,
    FORMAT(AVG(avg_transaction_value), 2) as avg_transaction_size,
    SUM(unique_customers) as total_customer_interactions,
    FORMAT(SUM(total_volume) / SUM(unique_customers), 2) as lifetime_value_per_customer,
    ROUND(AVG(data_quality_score), 1) as avg_data_quality,
    RANK() OVER (ORDER BY SUM(total_volume) DESC) as volume_rank,
    RANK() OVER (ORDER BY SUM(unique_customers) DESC) as customer_reach_rank
FROM daily_bank_aggregates
WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY bank_id
ORDER BY total_lifetime_volume DESC;

-- Query 5: Time series data for PowerBI dashboard
SELECT 
    transaction_date,
    bank_id,
    total_volume,
    transaction_count,
    avg_transaction_value,
    unique_customers,
    data_quality_score,
    DAYOFWEEK(transaction_date) as day_of_week,
    DAYNAME(transaction_date) as day_name,
    WEEK(transaction_date) as week_number,
    MONTH(transaction_date) as month_number,
    QUARTER(transaction_date) as quarter_number
FROM daily_bank_aggregates
WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
ORDER BY transaction_date DESC, bank_id;