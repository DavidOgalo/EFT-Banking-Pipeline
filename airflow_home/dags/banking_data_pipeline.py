"""
Main Airflow DAG for EFT Banking Pipeline
Ingests daily transactional data, transforms it, and loads to MySQL database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
import numpy as np
import logging
from typing import Dict, List
import os

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'banking_data_pipeline',
    default_args=default_args,
    description='Daily banking transaction data pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['banking', 'etl', 'daily']
)

# Configuration
DATA_SOURCE_PATH = "/opt/airflow/data/raw/"
PROCESSED_PATH = "/opt/airflow/data/processed/"
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "daily_bank_aggregates"

def generate_mock_data(**context):
    """
    Generate mock banking transaction data for testing
    In production, this would be replaced with S3 ingestion
    """
    execution_date = context['ds']
    logging.info(f"Generating mock data for {execution_date}")
    
    # Create realistic banking transaction data
    np.random.seed(42)  # For reproducible results
    
    banks = ['BNK001', 'BNK002', 'BNK003', 'BNK004', 'BNK005', 'BNK006', 'BNK007']
    transaction_types = ['TRANSFER', 'DEPOSIT', 'WITHDRAWAL', 'PAYMENT']
    
    n_transactions = np.random.randint(1000, 5000)
    
    data = {
        'transaction_id': [f"TXN{str(i).zfill(8)}" for i in range(n_transactions)],
        'bank_id': np.random.choice(banks, n_transactions),
        'customer_id': [f"CUST{str(np.random.randint(1000, 9999))}" for _ in range(n_transactions)],
        'transaction_type': np.random.choice(transaction_types, n_transactions),
        'amount': np.random.exponential(500, n_transactions).round(2),
        'transaction_date': execution_date,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Introduce some null values and data quality issues
    null_indices = np.random.choice(n_transactions, size=int(n_transactions * 0.02), replace=False)
    data['amount'] = list(data['amount'])
    for idx in null_indices[:len(null_indices)//2]:
        data['amount'][idx] = None
    
    df = pd.DataFrame(data)
    
    # Ensure directory exists
    os.makedirs(DATA_SOURCE_PATH, exist_ok=True)
    
    # Save to CSV
    file_path = f"{DATA_SOURCE_PATH}transactions_{execution_date}.csv"
    df.to_csv(file_path, index=False)
    
    logging.info(f"Generated {len(df)} transactions and saved to {file_path}")
    return file_path

def ingest_data(**context):
    """
    Ingest data from CSV file (simulating S3 ingestion)
    In production, this would use S3Hook or similar
    """
    execution_date = context['ds']
    file_path = f"{DATA_SOURCE_PATH}transactions_{execution_date}.csv"
    
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        
        # Store file path in XCom for next task
        context['task_instance'].xcom_push(key='raw_data_path', value=file_path)
        context['task_instance'].xcom_push(key='raw_record_count', value=len(df))
        
        return file_path
    except Exception as e:
        logging.error(f"Error ingesting data: {str(e)}")
        raise

def transform_data(**context):
    """
    Transform and clean the banking data
    """
    # Get file path from previous task
    raw_data_path = context['task_instance'].xcom_pull(key='raw_data_path')
    
    # Read raw data
    df = pd.read_csv(raw_data_path)
    logging.info(f"Starting transformation of {len(df)} records")
    
    # Data cleaning and validation
    initial_count = len(df)
    
    # 1. Clean null values
    df = df.dropna(subset=['amount', 'bank_id', 'customer_id'])
    logging.info(f"Removed {initial_count - len(df)} records with null critical values")
    
    # 2. Validate column types
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df.dropna(subset=['amount'])
    
    # Remove negative amounts (invalid transactions)
    df = df[df['amount'] > 0]
    
    # 3. Data type conversions
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # 4. Business rule validations
    # Remove extremely large transactions (potential data errors)
    df = df[df['amount'] <= 1000000]  # Max 1M per transaction
    
    logging.info(f"After cleaning: {len(df)} valid records")
    
    # 5. Aggregate daily transaction totals by bank_id
    daily_aggregates = df.groupby(['bank_id', 'transaction_date']).agg({
        'amount': ['sum', 'count', 'mean', 'std'],
        'customer_id': 'nunique',
        'transaction_type': lambda x: x.value_counts().to_dict()
    }).round(2)
    
    # Flatten column names
    daily_aggregates.columns = [
        'total_volume', 'transaction_count', 'avg_transaction_value', 
        'std_transaction_value', 'unique_customers', 'transaction_type_breakdown'
    ]
    
    daily_aggregates = daily_aggregates.reset_index()
    
    # Add metadata
    daily_aggregates['processed_at'] = datetime.now()
    daily_aggregates['data_quality_score'] = (len(df) / initial_count) * 100
    
    # Save processed data
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    processed_file = f"{PROCESSED_PATH}daily_aggregates_{context['ds']}.csv"
    daily_aggregates.to_csv(processed_file, index=False)
    
    logging.info(f"Transformation complete. Aggregated data saved to {processed_file}")
    
    # Store results in XCom
    context['task_instance'].xcom_push(key='processed_data_path', value=processed_file)
    context['task_instance'].xcom_push(key='processed_record_count', value=len(daily_aggregates))
    
    return processed_file

def load_to_mysql(**context):
    """
    Load transformed data into MySQL database
    """
    processed_data_path = context['task_instance'].xcom_pull(key='processed_data_path')
    
    # Read processed data
    df = pd.read_csv(processed_data_path)
    
    # Get MySQL connection
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        bank_id VARCHAR(50) NOT NULL,
        transaction_date DATE NOT NULL,
        total_volume DECIMAL(15,2) NOT NULL,
        transaction_count INT NOT NULL,
        avg_transaction_value DECIMAL(10,2),
        std_transaction_value DECIMAL(10,2),
        unique_customers INT,
        transaction_type_breakdown TEXT,
        processed_at DATETIME,
        data_quality_score DECIMAL(5,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_bank_date (bank_id, transaction_date)
    );
    """
    
    mysql_hook.run(create_table_sql)
    logging.info(f"Table {TABLE_NAME} created/verified")
    
    # Insert data (using INSERT IGNORE to handle duplicates)
    for _, row in df.iterrows():
        insert_sql = f"""
        INSERT IGNORE INTO {TABLE_NAME} 
        (bank_id, transaction_date, total_volume, transaction_count, 
         avg_transaction_value, std_transaction_value, unique_customers, 
         transaction_type_breakdown, processed_at, data_quality_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        mysql_hook.run(insert_sql, parameters=(
            row['bank_id'], row['transaction_date'], row['total_volume'],
            row['transaction_count'], row['avg_transaction_value'],
            row['std_transaction_value'], row['unique_customers'],
            str(row['transaction_type_breakdown']), row['processed_at'],
            row['data_quality_score']
        ))
    
    logging.info(f"Successfully loaded {len(df)} records into {TABLE_NAME}")

def data_quality_check(**context):
    """
    Perform data quality checks on the loaded data
    """
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    execution_date = context['ds']
    
    # Check 1: Verify data was loaded for current date
    count_sql = f"""
    SELECT COUNT(*) as record_count 
    FROM {TABLE_NAME} 
    WHERE transaction_date = '{execution_date}'
    """
    
    result = mysql_hook.get_first(count_sql)
    record_count = result[0] if result else 0
    
    if record_count == 0:
        raise ValueError(f"No records found for {execution_date}")
    
    # Check 2: Verify data quality scores
    quality_sql = f"""
    SELECT AVG(data_quality_score) as avg_quality 
    FROM {TABLE_NAME} 
    WHERE transaction_date = '{execution_date}'
    """
    
    quality_result = mysql_hook.get_first(quality_sql)
    avg_quality = quality_result[0] if quality_result else 0
    
    if avg_quality < 80:
        logging.warning(f"Data quality score is low: {avg_quality}%")
    
    logging.info(f"Data quality check passed. Records: {record_count}, Quality: {avg_quality}%")

# Define tasks
generate_data_task = PythonOperator(
    task_id='generate_mock_data',
    python_callable=generate_mock_data,
    dag=dag
)

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

# Task dependencies
generate_data_task >> ingest_task >> transform_task >> load_task >> quality_check_task