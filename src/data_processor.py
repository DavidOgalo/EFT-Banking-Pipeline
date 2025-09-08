"""
Advanced Data Transformation Module for EFT Banking Pipeline
Banking Transaction Data Processing and Quality Assurance
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
import json
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionType(Enum):
    TRANSFER = "TRANSFER"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    PAYMENT = "PAYMENT"

class DataQualityLevel(Enum):
    EXCELLENT = "EXCELLENT"  # 95-100%
    GOOD = "GOOD"           # 85-94%
    ACCEPTABLE = "ACCEPTABLE"  # 75-84%
    POOR = "POOR"           # Below 75%

@dataclass
class DataQualityReport:
    """Data quality assessment results"""
    total_records: int
    valid_records: int
    null_records: int
    invalid_amounts: int
    duplicate_records: int
    quality_score: float
    quality_level: DataQualityLevel
    anomaly_count: int
    processing_timestamp: datetime

class BankingDataProcessor:
    """
    Advanced banking data processor with comprehensive validation,
    cleaning, and aggregation capabilities
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._get_default_config()
        self.quality_report = None
        
    def _get_default_config(self) -> Dict:
        """Default configuration for data processing"""
        return {
            'max_transaction_amount': 1000000.0,
            'min_transaction_amount': 0.01,
            'required_columns': ['transaction_id', 'bank_id', 'customer_id', 'amount', 'transaction_date'],
            'anomaly_threshold_std': 3.0,
            'date_format': '%Y-%m-%d',
            'currency_precision': 2,
            'remove_duplicates': True,
            'handle_outliers': True
        }
    
    def validate_schema(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate if the dataframe has required columns and basic structure
        """
        errors = []
        required_cols = self.config['required_columns']
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        if df.empty:
            errors.append("Dataframe is empty")
            
        return len(errors) == 0, errors
    
    def clean_null_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Advanced null value handling with business logic
        """
        logger.info(f"Starting null value cleaning. Initial records: {len(df)}")
        initial_count = len(df)
        
        # Critical fields that cannot be null
        critical_fields = ['bank_id', 'customer_id', 'amount']
        
        # Log null counts before cleaning
        for field in critical_fields:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in {field}")
        
        # Remove records with null critical fields
        df_clean = df.dropna(subset=critical_fields)
        
        # Handle optional fields
        if 'transaction_type' in df.columns:
            df_clean['transaction_type'] = df_clean['transaction_type'].fillna('UNKNOWN')
        
        if 'description' in df.columns:
            df_clean['description'] = df_clean['description'].fillna('No description')
        
        removed_count = initial_count - len(df_clean)
        logger.info(f"Removed {removed_count} records with null critical values")
        
        return df_clean
    
    def validate_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Comprehensive column type validation and conversion
        """
        logger.info("Starting column type validation and conversion")
        df_typed = df.copy()
        
        # Amount validation and conversion
        logger.info("Processing amount column")
        df_typed['amount'] = pd.to_numeric(df_typed['amount'], errors='coerce')
        
        # Remove rows where amount conversion failed
        invalid_amounts = df_typed['amount'].isnull().sum()
        if invalid_amounts > 0:
            logger.warning(f"Found {invalid_amounts} invalid amount values")
            df_typed = df_typed.dropna(subset=['amount'])
        
        # Validate amount ranges
        min_amount = self.config['min_transaction_amount']
        max_amount = self.config['max_transaction_amount']
        
        invalid_range = ((df_typed['amount'] <= 0) | 
                        (df_typed['amount'] > max_amount)).sum()
        
        if invalid_range > 0:
            logger.warning(f"Found {invalid_range} amounts outside valid range")
            df_typed = df_typed[
                (df_typed['amount'] > 0) & 
                (df_typed['amount'] <= max_amount)
            ]
        
        # Round amounts to specified precision
        precision = self.config['currency_precision']
        df_typed['amount'] = df_typed['amount'].round(precision)
        
        # Date validation and conversion
        if 'transaction_date' in df_typed.columns:
            logger.info("Processing transaction_date column")
            df_typed['transaction_date'] = pd.to_datetime(
                df_typed['transaction_date'], 
                errors='coerce'
            )
            
            # Remove future dates (data quality issue)
            future_dates = (df_typed['transaction_date'] > datetime.now()).sum()
            if future_dates > 0:
                logger.warning(f"Found {future_dates} future dates, removing them")
                df_typed = df_typed[df_typed['transaction_date'] <= datetime.now()]
        
        # String field validation
        string_fields = ['bank_id', 'customer_id', 'transaction_id']
        for field in string_fields:
            if field in df_typed.columns:
                df_typed[field] = df_typed[field].astype(str).str.strip()
                # Remove empty strings
                df_typed = df_typed[df_typed[field] != '']
        
        # Transaction type validation
        if 'transaction_type' in df_typed.columns:
            valid_types = [t.value for t in TransactionType]
            invalid_types = ~df_typed['transaction_type'].isin(valid_types + ['UNKNOWN'])
            if invalid_types.any():
                logger.warning(f"Found {invalid_types.sum()} invalid transaction types")
                df_typed.loc[invalid_types, 'transaction_type'] = 'UNKNOWN'
        
        logger.info(f"Type validation complete. Records remaining: {len(df_typed)}")
        return df_typed
    
    def detect_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and handle duplicate transactions
        """
        if not self.config['remove_duplicates']:
            return df
        
        logger.info("Detecting duplicate transactions")
        initial_count = len(df)
        
        # Define duplicate criteria
        duplicate_columns = ['bank_id', 'customer_id', 'amount', 'transaction_date']
        available_columns = [col for col in duplicate_columns if col in df.columns]
        
        duplicates = df.duplicated(subset=available_columns, keep='first')
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate transactions")
            df_clean = df[~duplicates]
            logger.info(f"Removed {duplicate_count} duplicates. Records remaining: {len(df_clean)}")
            return df_clean
        
        return df
    
    def detect_anomalies(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Detect anomalous transactions using statistical methods
        """
        if not self.config['handle_outliers']:
            return df, pd.DataFrame()
        
        logger.info("Detecting anomalous transactions")
        
        # Z-score based anomaly detection for amounts
        threshold = self.config['anomaly_threshold_std']
        
        # Calculate z-scores by bank (each bank may have different patterns)
        df['amount_zscore'] = df.groupby('bank_id')['amount'].transform(
            lambda x: np.abs((x - x.mean()) / x.std()) if x.std() > 0 else 0
        )
        
        # Identify anomalies
        anomalies = df[df['amount_zscore'] > threshold].copy()
        normal_data = df[df['amount_zscore'] <= threshold].copy()
        
        # Clean up temporary column
        if 'amount_zscore' in normal_data.columns:
            normal_data = normal_data.drop('amount_zscore', axis=1)
        
        anomaly_count = len(anomalies)
        if anomaly_count > 0:
            logger.warning(f"Detected {anomaly_count} anomalous transactions")
            # You might want to store these for further investigation
            anomalies['anomaly_type'] = 'statistical_outlier'
            anomalies['detected_at'] = datetime.now()
        
        return normal_data, anomalies
    
    def generate_quality_report(self, original_df: pd.DataFrame, 
                              cleaned_df: pd.DataFrame,
                              anomalies_df: pd.DataFrame) -> DataQualityReport:
        """
        Generate comprehensive data quality report
        """
        total_records = len(original_df)
        valid_records = len(cleaned_df)
        
        # Calculate various quality metrics
        null_records = 0
        for col in self.config['required_columns']:
            if col in original_df.columns:
                null_records += original_df[col].isnull().sum()
        
        invalid_amounts = 0
        if 'amount' in original_df.columns:
            invalid_amounts = pd.to_numeric(original_df['amount'], errors='coerce').isnull().sum()
        
        duplicate_columns = ['bank_id', 'customer_id', 'amount', 'transaction_date']
        available_dup_cols = [col for col in duplicate_columns if col in original_df.columns]
        duplicate_records = original_df.duplicated(subset=available_dup_cols).sum()
        
        # Calculate quality score
        quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
        
        # Determine quality level
        if quality_score >= 95:
            quality_level = DataQualityLevel.EXCELLENT
        elif quality_score >= 85:
            quality_level = DataQualityLevel.GOOD
        elif quality_score >= 75:
            quality_level = DataQualityLevel.ACCEPTABLE
        else:
            quality_level = DataQualityLevel.POOR
        
        report = DataQualityReport(
            total_records=total_records,
            valid_records=valid_records,
            null_records=null_records,
            invalid_amounts=invalid_amounts,
            duplicate_records=duplicate_records,
            quality_score=round(quality_score, 2),
            quality_level=quality_level,
            anomaly_count=len(anomalies_df),
            processing_timestamp=datetime.now()
        )
        
        self.quality_report = report
        return report
    
    def aggregate_daily_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate transactions by bank and date with comprehensive metrics
        """
        logger.info("Aggregating daily transactions by bank")
        
        # Ensure we have the required columns
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        # Base aggregation
        agg_funcs = {
            'amount': ['sum', 'count', 'mean', 'std', 'median', 'min', 'max'],
            'customer_id': 'nunique',
            'transaction_id': 'nunique'
        }
        
        # Add transaction type breakdown if available
        if 'transaction_type' in df.columns:
            agg_funcs['transaction_type'] = lambda x: x.value_counts().to_dict()
        
        # Perform aggregation
        daily_agg = df.groupby(['bank_id', 'transaction_date']).agg(agg_funcs)
        
        # Flatten multi-level columns
        daily_agg.columns = [
            'total_volume', 'transaction_count', 'avg_transaction_value', 
            'std_transaction_value', 'median_transaction_value', 
            'min_transaction_value', 'max_transaction_value',
            'unique_customers', 'unique_transaction_ids'
        ]
        
        if 'transaction_type' in df.columns:
            daily_agg['transaction_type_breakdown'] = df.groupby(['bank_id', 'transaction_date'])['transaction_type'].apply(
                lambda x: x.value_counts().to_dict()
            )
        
        daily_agg = daily_agg.reset_index()
        
        # Add derived metrics
        daily_agg['avg_transactions_per_customer'] = (
            daily_agg['transaction_count'] / daily_agg['unique_customers']
        ).round(2)
        
        daily_agg['avg_value_per_customer'] = (
            daily_agg['total_volume'] / daily_agg['unique_customers']
        ).round(2)
        
        # Add processing metadata
        daily_agg['processed_at'] = datetime.now()
        daily_agg['data_quality_score'] = self.quality_report.quality_score if self.quality_report else 100.0
        
        # Handle infinite values
        daily_agg = daily_agg.replace([np.inf, -np.inf], np.nan)
        daily_agg = daily_agg.fillna(0)
        
        # Round numerical columns
        numeric_columns = daily_agg.select_dtypes(include=[np.number]).columns
        daily_agg[numeric_columns] = daily_agg[numeric_columns].round(2)
        
        logger.info(f"Aggregation complete. Generated {len(daily_agg)} bank-date records")
        
        return daily_agg
    
    def process_banking_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, DataQualityReport, pd.DataFrame]:
        """
        Main processing pipeline that orchestrates all data transformations
        """
        logger.info("Starting comprehensive banking data processing")
        logger.info(f"Input data shape: {df.shape}")
        
        # Store original for quality reporting
        original_df = df.copy()
        
        # Step 1: Schema validation
        is_valid, errors = self.validate_schema(df)
        if not is_valid:
            raise ValueError(f"Schema validation failed: {errors}")
        
        # Step 2: Clean null values
        df_clean = self.clean_null_values(df)
        
        # Step 3: Validate and convert column types
        df_typed = self.validate_column_types(df_clean)
        
        # Step 4: Handle duplicates
        df_deduped = self.detect_duplicates(df_typed)
        
        # Step 5: Detect anomalies
        df_final, anomalies_df = self.detect_anomalies(df_deduped)
        
        # Step 6: Generate quality report
        quality_report = self.generate_quality_report(original_df, df_final, anomalies_df)
        
        # Step 7: Aggregate data
        aggregated_data = self.aggregate_daily_transactions(df_final)
        
        logger.info("Processing complete!")
        logger.info(f"Final data shape: {aggregated_data.shape}")
        logger.info(f"Data quality score: {quality_report.quality_score}%")
        
        return aggregated_data, quality_report, anomalies_df
    
    def save_processing_results(self, aggregated_data: pd.DataFrame, 
                              quality_report: DataQualityReport,
                              anomalies_df: pd.DataFrame,
                              output_path: str = "./output/"):
        """
        Save all processing results to files
        """
        import os
        os.makedirs(output_path, exist_ok=True)
        
        # Save aggregated data
        agg_file = f"{output_path}daily_bank_aggregates.csv"
        aggregated_data.to_csv(agg_file, index=False)
        logger.info(f"Saved aggregated data to {agg_file}")
        
        # Save quality report
        quality_file = f"{output_path}data_quality_report.json"
        quality_dict = {
            'total_records': quality_report.total_records,
            'valid_records': quality_report.valid_records,
            'null_records': quality_report.null_records,
            'invalid_amounts': quality_report.invalid_amounts,
            'duplicate_records': quality_report.duplicate_records,
            'quality_score': quality_report.quality_score,
            'quality_level': quality_report.quality_level.value,
            'anomaly_count': quality_report.anomaly_count,
            'processing_timestamp': quality_report.processing_timestamp.isoformat()
        }
        
        with open(quality_file, 'w') as f:
            json.dump(quality_dict, f, indent=2)
        logger.info(f"Saved quality report to {quality_file}")
        
        # Save anomalies if any
        if not anomalies_df.empty:
            anomaly_file = f"{output_path}detected_anomalies.csv"
            anomalies_df.to_csv(anomaly_file, index=False)
            logger.info(f"Saved {len(anomalies_df)} anomalies to {anomaly_file}")

# Example usage and testing functions
def create_sample_banking_data(n_records: int = 10000) -> pd.DataFrame:
    """
    Create realistic sample banking data for testing
    """
    np.random.seed(42)
    
    banks = ['BNK001', 'BNK002', 'BNK003', 'BNK004', 'BNK005']
    transaction_types = [t.value for t in TransactionType]
    
    data = {
        'transaction_id': [f"TXN{str(i).zfill(8)}" for i in range(n_records)],
        'bank_id': np.random.choice(banks, n_records),
        'customer_id': [f"CUST{str(np.random.randint(1000, 9999))}" for _ in range(n_records)],
        'transaction_type': np.random.choice(transaction_types, n_records),
        'amount': np.random.exponential(500, n_records).round(2),
        'transaction_date': pd.date_range(start='2025-08-01', periods=30, freq='D').repeat(n_records // 30 + 1)[:n_records],
        'description': [f"Transaction {i}" for i in range(n_records)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce some data quality issues for testing
    # Add some null values
    null_indices = np.random.choice(n_records, size=int(n_records * 0.05), replace=False)
    df.loc[null_indices[:len(null_indices)//3], 'amount'] = None
    df.loc[null_indices[len(null_indices)//3:2*len(null_indices)//3], 'bank_id'] = None
    
    # Add some invalid amounts
    df.loc[np.random.choice(n_records, size=50, replace=False), 'amount'] = -100
    df.loc[np.random.choice(n_records, size=30, replace=False), 'amount'] = 2000000  # Too large
    
    # Add some duplicates
    duplicate_rows = df.sample(100)
    df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    return df

def main():
    """
    Example usage of the BankingDataProcessor
    """
    # Create sample data
    logger.info("Creating sample banking data")
    sample_data = create_sample_banking_data(5000)
    
    # Initialize processor
    processor = BankingDataProcessor()
    
    # Process the data
    aggregated_data, quality_report, anomalies = processor.process_banking_data(sample_data)
    
    # Save results
    processor.save_processing_results(aggregated_data, quality_report, anomalies)
    
    # Print summary
    print("\n" + "="*50)
    print("PROCESSING SUMMARY")
    print("="*50)
    print(f"Original records: {quality_report.total_records:,}")
    print(f"Valid records: {quality_report.valid_records:,}")
    print(f"Quality score: {quality_report.quality_score}%")
    print(f"Quality level: {quality_report.quality_level.value}")
    print(f"Anomalies detected: {quality_report.anomaly_count}")
    print(f"Final aggregated records: {len(aggregated_data)}")
    print("="*50)

if __name__ == "__main__":
    main()