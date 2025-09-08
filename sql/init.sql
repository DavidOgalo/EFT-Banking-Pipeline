-- Ensure database exists and use it
CREATE DATABASE IF NOT EXISTS eft_banking;
USE eft_banking;

-- Create bank_profiles table for dimensional data
CREATE TABLE IF NOT EXISTS bank_profiles (
    bank_id VARCHAR(50) PRIMARY KEY,
    bank_name VARCHAR(200) NOT NULL,
    bank_type ENUM('Commercial', 'Investment', 'Community', 'Online') DEFAULT 'Commercial',
    headquarters_city VARCHAR(100),
    headquarters_country VARCHAR(100) DEFAULT 'Kenya',
    established_year INT,
    total_assets DECIMAL(20,2),
    employee_count INT,
    branch_count INT,
    is_active BOOLEAN DEFAULT TRUE,
    risk_rating ENUM('Low', 'Medium', 'High') DEFAULT 'Medium',
    regulatory_tier ENUM('Tier1', 'Tier2', 'Tier3') DEFAULT 'Tier2',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert sample bank profiles
INSERT IGNORE INTO bank_profiles (bank_id, bank_name, bank_type, headquarters_city, established_year, total_assets, employee_count, branch_count, risk_rating, regulatory_tier) VALUES
('BNK001', 'Premier Bank Kenya', 'Commercial', 'Nairobi', 1995, 125000000000.00, 2500, 120, 'Low', 'Tier1'),
('BNK002', 'Community First Bank', 'Community', 'Mombasa', 2001, 45000000000.00, 850, 45, 'Medium', 'Tier2'),
('BNK003', 'Metro Financial Services', 'Commercial', 'Nairobi', 1988, 98000000000.00, 1800, 95, 'Low', 'Tier1'),
('BNK004', 'Regional Trust Bank', 'Commercial', 'Kisumu', 2005, 32000000000.00, 650, 35, 'Medium', 'Tier2'),
('BNK005', 'Digital Banking Corp', 'Online', 'Nairobi', 2015, 78000000000.00, 400, 12, 'Medium', 'Tier2'),
('BNK006', 'Heritage Bank Limited', 'Commercial', 'Nakuru', 1978, 25000000000.00, 420, 28, 'Low', 'Tier3'),
('BNK007', 'Innovation Bank', 'Investment', 'Nairobi', 2010, 67000000000.00, 320, 18, 'Medium', 'Tier2');

-- Create daily_bank_aggregates table
CREATE TABLE IF NOT EXISTS daily_bank_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bank_id VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    total_volume DECIMAL(15,2) NOT NULL,
    transaction_count INT NOT NULL,
    avg_transaction_value DECIMAL(10,2),
    std_transaction_value DECIMAL(10,2),
    median_transaction_value DECIMAL(10,2),
    min_transaction_value DECIMAL(10,2),
    max_transaction_value DECIMAL(10,2),
    unique_customers INT,
    unique_transaction_ids INT,
    transaction_type_breakdown TEXT,
    avg_transactions_per_customer DECIMAL(8,2),
    avg_value_per_customer DECIMAL(12,2),
    processed_at DATETIME,
    data_quality_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_bank_date (bank_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_volume (total_volume),
    INDEX idx_created_at (created_at),
    UNIQUE KEY unique_bank_date (bank_id, transaction_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create data_quality_log table for monitoring
CREATE TABLE IF NOT EXISTS data_quality_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    processing_date DATE NOT NULL,
    bank_id VARCHAR(50),
    total_records_processed INT,
    valid_records INT,
    invalid_records INT,
    null_records INT,
    duplicate_records INT,
    anomaly_records INT,
    quality_score DECIMAL(5,2),
    quality_level ENUM('EXCELLENT', 'GOOD', 'ACCEPTABLE', 'POOR'),
    processing_duration_seconds INT,
    pipeline_status ENUM('SUCCESS', 'FAILED', 'PARTIAL') DEFAULT 'SUCCESS',
    error_message TEXT,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_processing_date (processing_date),
    INDEX idx_bank_quality (bank_id, quality_score),
    INDEX idx_status (pipeline_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create anomalies table for tracking detected anomalies
CREATE TABLE IF NOT EXISTS transaction_anomalies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_date DATE NOT NULL,
    bank_id VARCHAR(50) NOT NULL,
    anomaly_type ENUM('statistical_outlier', 'business_rule_violation', 'data_quality_issue') NOT NULL,
    anomaly_severity ENUM('LOW', 'MEDIUM', 'HIGH', 'CRITICAL') DEFAULT 'MEDIUM',
    actual_value DECIMAL(15,2),
    expected_range_min DECIMAL(15,2),
    expected_range_max DECIMAL(15,2),
    z_score DECIMAL(8,4),
    anomaly_description TEXT,
    is_investigated BOOLEAN DEFAULT FALSE,
    investigation_notes TEXT,
    resolved_at DATETIME,
    detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date_bank (transaction_date, bank_id),
    INDEX idx_severity (anomaly_severity),
    INDEX idx_investigated (is_investigated),
    FOREIGN KEY (bank_id) REFERENCES bank_profiles(bank_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create monthly_bank_summary table for aggregated reporting
CREATE TABLE IF NOT EXISTS monthly_bank_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bank_id VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    total_monthly_volume DECIMAL(18,2),
    total_monthly_transactions INT,
    avg_daily_volume DECIMAL(15,2),
    avg_daily_transactions DECIMAL(10,2),
    unique_monthly_customers INT,
    avg_transaction_value DECIMAL(10,2),
    max_daily_volume DECIMAL(15,2),
    min_daily_volume DECIMAL(15,2),
    volume_volatility DECIMAL(10,4),
    active_days_in_month INT,
    avg_data_quality_score DECIMAL(5,2),
    anomaly_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_bank_month (bank_id, year, month),
    INDEX idx_year_month (year, month),
    INDEX idx_volume (total_monthly_volume),
    FOREIGN KEY (bank_id) REFERENCES bank_profiles(bank_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create pipeline_execution_log for monitoring pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    execution_date DATE NOT NULL,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100),
    execution_start DATETIME,
    execution_end DATETIME,
    execution_duration_minutes DECIMAL(8,2),
    status ENUM('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED', 'RETRY') NOT NULL,
    records_processed INT,
    records_loaded INT,
    error_details TEXT,
    log_level ENUM('INFO', 'WARNING', 'ERROR', 'CRITICAL') DEFAULT 'INFO',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_execution_date (execution_date),
    INDEX idx_dag_task (dag_id, task_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create views for common queries

-- View: Daily volume summary across all banks
CREATE OR REPLACE VIEW vw_daily_volume_summary AS
SELECT 
    transaction_date,
    COUNT(DISTINCT bank_id) as active_banks,
    SUM(total_volume) as system_total_volume,
    SUM(transaction_count) as system_total_transactions,
    SUM(unique_customers) as system_total_customers,
    AVG(avg_transaction_value) as system_avg_transaction_value,
    AVG(data_quality_score) as avg_data_quality_score,
    MAX(total_volume) as max_bank_volume,
    MIN(total_volume) as min_bank_volume,
    STDDEV(total_volume) as volume_std_deviation
FROM daily_bank_aggregates
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- View: Bank performance ranking
CREATE OR REPLACE VIEW vw_bank_performance_ranking AS
SELECT 
    bp.bank_id,
    bp.bank_name,
    bp.bank_type,
    bp.regulatory_tier,
    COUNT(DISTINCT dba.transaction_date) as total_active_days,
    SUM(dba.total_volume) as lifetime_total_volume,
    AVG(dba.total_volume) as avg_daily_volume,
    SUM(dba.transaction_count) as lifetime_transactions,
    AVG(dba.avg_transaction_value) as avg_transaction_value,
    SUM(dba.unique_customers) as total_customer_interactions,
    AVG(dba.data_quality_score) as avg_data_quality,
    RANK() OVER (ORDER BY SUM(dba.total_volume) DESC) as volume_rank,
    RANK() OVER (ORDER BY AVG(dba.data_quality_score) DESC) as quality_rank,
    RANK() OVER (ORDER BY SUM(dba.unique_customers) DESC) as customer_reach_rank
FROM bank_profiles bp
LEFT JOIN daily_bank_aggregates dba ON bp.bank_id = dba.bank_id
WHERE dba.transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY bp.bank_id, bp.bank_name, bp.bank_type, bp.regulatory_tier
ORDER BY volume_rank;

-- View: Recent anomalies with bank details
CREATE OR REPLACE VIEW vw_recent_anomalies AS
SELECT 
    ta.id,
    ta.transaction_date,
    bp.bank_name,
    ta.bank_id,
    ta.anomaly_type,
    ta.anomaly_severity,
    ta.actual_value,
    ta.expected_range_min,
    ta.expected_range_max,
    ta.z_score,
    ta.anomaly_description,
    ta.is_investigated,
    ta.detected_at,
    DATEDIFF(CURDATE(), ta.transaction_date) as days_since_occurrence
FROM transaction_anomalies ta
JOIN bank_profiles bp ON ta.bank_id = bp.bank_id
WHERE ta.detected_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY ta.detected_at DESC;

-- Create stored procedures for common operations

DELIMITER //

-- Procedure to calculate monthly summaries
CREATE PROCEDURE sp_calculate_monthly_summary(IN target_year INT, IN target_month INT)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    DELETE FROM monthly_bank_summary 
    WHERE year = target_year AND month = target_month;
    
    INSERT INTO monthly_bank_summary (
        bank_id, year, month, month_name, total_monthly_volume,
        total_monthly_transactions, avg_daily_volume, avg_daily_transactions,
        unique_monthly_customers, avg_transaction_value, max_daily_volume,
        min_daily_volume, volume_volatility, active_days_in_month,
        avg_data_quality_score, anomaly_count
    )
    SELECT 
        dba.bank_id,
        target_year,
        target_month,
        MONTHNAME(STR_TO_DATE(target_month, '%m')),
        SUM(dba.total_volume),
        SUM(dba.transaction_count),
        AVG(dba.total_volume),
        AVG(dba.transaction_count),
        SUM(dba.unique_customers),
        AVG(dba.avg_transaction_value),
        MAX(dba.total_volume),
        MIN(dba.total_volume),
        STDDEV(dba.total_volume),
        COUNT(DISTINCT dba.transaction_date),
        AVG(dba.data_quality_score),
        COALESCE(anomaly_counts.anomaly_count, 0)
    FROM daily_bank_aggregates dba
    LEFT JOIN (
        SELECT bank_id, COUNT(*) as anomaly_count
        FROM transaction_anomalies
        WHERE YEAR(transaction_date) = target_year 
        AND MONTH(transaction_date) = target_month
        GROUP BY bank_id
    ) anomaly_counts ON dba.bank_id = anomaly_counts.bank_id
    WHERE YEAR(dba.transaction_date) = target_year 
    AND MONTH(dba.transaction_date) = target_month
    GROUP BY dba.bank_id;
    
    COMMIT;
    
    SELECT CONCAT('Monthly summary calculated for ', target_year, '-', 
                  LPAD(target_month, 2, '0'), '. Records processed: ', 
                  ROW_COUNT()) as result;
END //

-- Procedure to identify and log anomalies
CREATE PROCEDURE sp_detect_anomalies(IN analysis_date DATE)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_bank_id VARCHAR(50);
    DECLARE v_actual_volume DECIMAL(15,2);
    DECLARE v_avg_volume DECIMAL(15,2);
    DECLARE v_std_volume DECIMAL(15,2);
    DECLARE v_z_score DECIMAL(8,4);
    
    DECLARE anomaly_cursor CURSOR FOR
        SELECT 
            bank_id,
            total_volume,
            avg_volume,
            std_volume,
            ABS(total_volume - avg_volume) / NULLIF(std_volume, 0) as z_score
        FROM (
            SELECT 
                current_day.bank_id,
                current_day.total_volume,
                historical.avg_volume,
                historical.std_volume
            FROM daily_bank_aggregates current_day
            JOIN (
                SELECT 
                    bank_id,
                    AVG(total_volume) as avg_volume,
                    STDDEV(total_volume) as std_volume
                FROM daily_bank_aggregates
                WHERE transaction_date BETWEEN DATE_SUB(analysis_date, INTERVAL 30 DAY) 
                AND DATE_SUB(analysis_date, INTERVAL 1 DAY)
                GROUP BY bank_id
                HAVING COUNT(*) >= 10
            ) historical ON current_day.bank_id = historical.bank_id
            WHERE current_day.transaction_date = analysis_date
        ) analysis
        WHERE ABS(total_volume - avg_volume) / NULLIF(std_volume, 0) > 2.0;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN anomaly_cursor;
    
    anomaly_loop: LOOP
        FETCH anomaly_cursor INTO v_bank_id, v_actual_volume, v_avg_volume, v_std_volume, v_z_score;
        IF done THEN
            LEAVE anomaly_loop;
        END IF;
        
        INSERT INTO transaction_anomalies (
            transaction_date, bank_id, anomaly_type, anomaly_severity,
            actual_value, expected_range_min, expected_range_max,
            z_score, anomaly_description
        ) VALUES (
            analysis_date,
            v_bank_id,
            'statistical_outlier',
            CASE 
                WHEN v_z_score > 3.0 THEN 'CRITICAL'
                WHEN v_z_score > 2.5 THEN 'HIGH'
                ELSE 'MEDIUM'
            END,
            v_actual_volume,
            v_avg_volume - (2 * v_std_volume),
            v_avg_volume + (2 * v_std_volume),
            v_z_score,
            CONCAT('Volume deviation of ', ROUND(v_z_score, 2), ' standard deviations from 30-day average')
        ) ON DUPLICATE KEY UPDATE
            z_score = VALUES(z_score),
            anomaly_description = VALUES(anomaly_description),
            detected_at = CURRENT_TIMESTAMP;
    END LOOP;
    
    CLOSE anomaly_cursor;
    
    SELECT CONCAT('Anomaly detection completed for ', analysis_date, 
                  '. Anomalies found: ', ROW_COUNT()) as result;
END //

DELIMITER ;

-- Create triggers for data quality monitoring

DELIMITER //

-- Trigger to log data quality after each insert
CREATE TRIGGER tr_daily_aggregates_quality_check
    AFTER INSERT ON daily_bank_aggregates
    FOR EACH ROW
BEGIN
    INSERT INTO data_quality_log (
        processing_date, bank_id, total_records_processed,
        valid_records, quality_score, quality_level,
        pipeline_status
    ) VALUES (
        NEW.transaction_date,
        NEW.bank_id,
        1,
        1,
        NEW.data_quality_score,
        CASE 
            WHEN NEW.data_quality_score >= 95 THEN 'EXCELLENT'
            WHEN NEW.data_quality_score >= 85 THEN 'GOOD'
            WHEN NEW.data_quality_score >= 75 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END,
        'SUCCESS'
    ) ON DUPLICATE KEY UPDATE
        total_records_processed = total_records_processed + 1,
        valid_records = valid_records + 1,
        processed_at = CURRENT_TIMESTAMP;
END //

DELIMITER ;

-- Insert initial data for testing
INSERT IGNORE INTO data_quality_log (processing_date, bank_id, total_records_processed, valid_records, quality_score, quality_level, pipeline_status) VALUES
('2025-09-07', 'BNK001', 100, 98, 98.0, 'EXCELLENT', 'SUCCESS'),
('2025-09-07', 'BNK002', 75, 73, 97.3, 'EXCELLENT', 'SUCCESS'),
('2025-09-07', 'BNK003', 120, 118, 98.3, 'EXCELLENT', 'SUCCESS');

-- Create indexes for performance
CREATE INDEX idx_daily_agg_composite ON daily_bank_aggregates(bank_id, transaction_date, total_volume);
CREATE INDEX idx_anomaly_severity_date ON transaction_anomalies(anomaly_severity, transaction_date);
CREATE INDEX idx_quality_log_date_score ON data_quality_log(processing_date, quality_score);

-- Grant permissions for application user
GRANT SELECT, INSERT, UPDATE, DELETE ON eft_banking.* TO 'eft_user'@'%';
GRANT EXECUTE ON PROCEDURE eft_banking.sp_calculate_monthly_summary TO 'eft_user'@'%';
GRANT EXECUTE ON PROCEDURE eft_banking.sp_detect_anomalies TO 'eft_user'@'%';

FLUSH PRIVILEGES;

-- Create daily_bank_aggregates table
CREATE TABLE IF NOT EXISTS daily_bank_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bank_id VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    total_volume DECIMAL(15,2) NOT NULL,
    transaction_count INT NOT NULL,
    avg_transaction_value DECIMAL(10,2),
    std_transaction_value DECIMAL(10,2),
    median_transaction_value DECIMAL(10,2),
    min_transaction_value DECIMAL(10,2),
    max_transaction_value DECIMAL(10,2),
    unique_customers INT,
    unique_transaction_ids INT,
    transaction_type_breakdown TEXT,
    avg_transactions_per_customer DECIMAL(8,2),
    avg_value_per_customer DECIMAL(12,2),
    processed_at DATETIME,
    data_quality_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_bank_date (bank_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_volume (total_volume),
    INDEX idx_created_at (created_at),
    UNIQUE KEY unique_bank_date (bank_id, transaction_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create bank_profiles table for dimensional data
CREATE TABLE IF NOT EXISTS bank_profiles (
    bank_id VARCHAR(50) PRIMARY KEY,
    bank_name VARCHAR(200) NOT NULL,
    bank_type ENUM('Commercial', 'Investment', 'Community', 'Online') DEFAULT 'Commercial',
    headquarters_city VARCHAR(100),
    headquarters_country VARCHAR(100) DEFAULT 'Kenya',
    established_year INT,
    total_assets DECIMAL(20,2),
    employee_count INT,
    branch_count INT,
    is_active BOOLEAN DEFAULT TRUE,
    risk_rating ENUM('Low', 'Medium', 'High') DEFAULT 'Medium',
    regulatory_tier ENUM('Tier1', 'Tier2', 'Tier3') DEFAULT 'Tier2',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert sample bank profiles
INSERT IGNORE INTO bank_profiles (bank_id, bank_name, bank_type, headquarters_city, established_year, total_assets, employee_count, branch_count, risk_rating, regulatory_tier) VALUES
('BNK001', 'Premier Bank Kenya', 'Commercial', 'Nairobi', 1995, 125000000000.00, 2500, 120, 'Low', 'Tier1'),
('BNK002', 'Community First Bank', 'Community', 'Mombasa', 2001, 45000000000.00, 850, 45, 'Medium', 'Tier2'),
('BNK003', 'Metro Financial Services', 'Commercial', 'Nairobi', 1988, 98000000000.00, 1800, 95, 'Low', 'Tier1'),
('BNK004', 'Regional Trust Bank', 'Commercial', 'Kisumu', 2005, 32000000000.00, 650, 35, 'Medium', 'Tier2'),
('BNK005', 'Digital Banking Corp', 'Online', 'Nairobi', 2015, 78000000000.00, 400, 12, 'Medium', 'Tier2'),
('BNK006', 'Heritage Bank Limited', 'Commercial', 'Nakuru', 1978, 25000000000.00, 420, 28, 'Low', 'Tier3'),
('BNK007', 'Innovation Bank', 'Investment', 'Nairobi', 2010, 67000000000.00, 320, 18, 'Medium', 'Tier2');

-- Create