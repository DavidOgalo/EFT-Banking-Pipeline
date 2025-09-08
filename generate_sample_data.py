"""
Sample Data Generator for PowerBI Dashboard
EFT Banking Pipeline Analytics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json

def generate_comprehensive_banking_data(days_back=180, base_daily_volume=50000):
    """
    Generate comprehensive banking data for PowerBI dashboard
    """
    np.random.seed(42)
    random.seed(42)
    
    # Bank configuration with realistic profiles
    banks = {
        'BNK001': {'name': 'Premier Bank', 'size_multiplier': 2.0, 'volatility': 0.15},
        'BNK002': {'name': 'Community First Bank', 'size_multiplier': 1.5, 'volatility': 0.20},
        'BNK003': {'name': 'Metro Financial', 'size_multiplier': 1.8, 'volatility': 0.12},
        'BNK004': {'name': 'Regional Trust Bank', 'size_multiplier': 1.2, 'volatility': 0.25},
        'BNK005': {'name': 'Digital Banking Corp', 'size_multiplier': 2.2, 'volatility': 0.18},
        'BNK006': {'name': 'Heritage Bank', 'size_multiplier': 0.8, 'volatility': 0.10},
        'BNK007': {'name': 'Innovation Bank', 'size_multiplier': 1.6, 'volatility': 0.22}
    }
    
    # Generate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    all_data = []
    
    for date in date_range:
        day_of_week = date.dayofweek
        month = date.month
        
        # Day of week effect (lower on weekends)
        day_multiplier = 1.0
        if day_of_week == 5:  # Saturday
            day_multiplier = 0.6
        elif day_of_week == 6:  # Sunday
            day_multiplier = 0.3
        elif day_of_week == 4:  # Friday
            day_multiplier = 1.2
        
        # Seasonal effects
        seasonal_multiplier = 1.0
        if month in [11, 12]:  # Holiday season
            seasonal_multiplier = 1.3
        elif month in [6, 7, 8]:  # Mid Year
            seasonal_multiplier = 0.9
        elif month == 1:  # January (post-holiday)
            seasonal_multiplier = 0.8
        
        # Add some random events (market volatility)
        event_multiplier = 1.0
        if random.random() < 0.05:  # 5% chance of significant event
            event_multiplier = random.choice([0.4, 0.5, 1.8, 2.2])  # Crisis or boom
        
        for bank_id, bank_info in banks.items():
            base_volume = base_daily_volume * bank_info['size_multiplier']
            volatility = bank_info['volatility']
            
            # Apply all multipliers
            daily_base = base_volume * day_multiplier * seasonal_multiplier * event_multiplier
            
            # Add random variation
            daily_volume = daily_base * (1 + np.random.normal(0, volatility))
            daily_volume = max(daily_volume, base_volume * 0.1)  # Minimum floor
            
            # Calculate other metrics
            transaction_count = int(daily_volume / np.random.uniform(80, 150))
            avg_transaction = daily_volume / transaction_count if transaction_count > 0 else 0
            
            # Customer metrics
            unique_customers = int(transaction_count * np.random.uniform(0.6, 0.9))
            avg_value_per_customer = daily_volume / unique_customers if unique_customers > 0 else 0
            
            # Data quality (occasional issues)
            data_quality_score = np.random.uniform(92, 100)
            if random.random() < 0.1:  # 10% chance of quality issues
                data_quality_score = np.random.uniform(75, 92)
            
            # Transaction type breakdown
            type_distribution = {
                'TRANSFER': 0.35,
                'DEPOSIT': 0.25,
                'WITHDRAWAL': 0.20,
                'PAYMENT': 0.20
            }
            
            # Add some variation to transaction types by bank
            if 'Digital' in bank_info['name']:
                type_distribution['PAYMENT'] += 0.1
                type_distribution['TRANSFER'] -= 0.1
            elif 'Heritage' in bank_info['name']:
                type_distribution['DEPOSIT'] += 0.1
                type_distribution['PAYMENT'] -= 0.1
            
            all_data.append({
                'transaction_date': date.strftime('%Y-%m-%d'),
                'bank_id': bank_id,
                'bank_name': bank_info['name'],
                'total_volume': round(daily_volume, 2),
                'transaction_count': transaction_count,
                'avg_transaction_value': round(avg_transaction, 2),
                'unique_customers': unique_customers,
                'avg_value_per_customer': round(avg_value_per_customer, 2),
                'data_quality_score': round(data_quality_score, 1),
                'day_of_week': day_of_week + 1,  # 1=Monday, 7=Sunday
                'day_name': date.strftime('%A'),
                'month': month,
                'month_name': date.strftime('%B'),
                'quarter': f'Q{((month-1)//3)+1}',
                'year': date.year,
                'week_number': date.isocalendar()[1],
                'is_weekend': day_of_week >= 5,
                'transfer_volume': round(daily_volume * type_distribution['TRANSFER'], 2),
                'deposit_volume': round(daily_volume * type_distribution['DEPOSIT'], 2),
                'withdrawal_volume': round(daily_volume * type_distribution['WITHDRAWAL'], 2),
                'payment_volume': round(daily_volume * type_distribution['PAYMENT'], 2),
                'volatility_score': round(volatility * 100, 1),
                'market_segment': 'Large' if bank_info['size_multiplier'] > 1.5 else 'Medium' if bank_info['size_multiplier'] > 1.0 else 'Small'
            })
    
    return pd.DataFrame(all_data)

def generate_anomaly_data():
    """
    Generate anomaly detection sample data
    """
    # Get the main dataset
    df = generate_comprehensive_banking_data()
    
    # Calculate anomaly thresholds
    df['volume_7day_avg'] = df.groupby('bank_id')['total_volume'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    df['volume_7day_std'] = df.groupby('bank_id')['total_volume'].transform(
        lambda x: x.rolling(window=7, min_periods=1).std()
    )
    
    # Define anomaly status
    def classify_anomaly(row):
        if pd.isna(row['volume_7day_std']) or row['volume_7day_std'] == 0:
            return 'Normal'
        
        z_score = abs(row['total_volume'] - row['volume_7day_avg']) / row['volume_7day_std']
        
        if z_score > 2.5:
            return 'High Anomaly'
        elif z_score > 1.5:
            return 'Moderate Anomaly'
        elif z_score < -2.5:
            return 'Low Anomaly'
        elif z_score < -1.5:
            return 'Below Normal'
        else:
            return 'Normal'
    
    df['anomaly_status'] = df.apply(classify_anomaly, axis=1)
    df['z_score'] = abs(df['total_volume'] - df['volume_7day_avg']) / df['volume_7day_std'].fillna(1)
    
    return df

def generate_customer_analysis_data():
    """
    Generate customer-centric analysis data
    """
    df = generate_comprehensive_banking_data()
    
    # Monthly customer analysis
    monthly_data = []
    
    for _, row in df.iterrows():
        # Generate customer segments
        high_value_customers = int(row['unique_customers'] * 0.15)
        medium_value_customers = int(row['unique_customers'] * 0.35)
        regular_customers = row['unique_customers'] - high_value_customers - medium_value_customers
        
        # Value distribution
        high_value_volume = row['total_volume'] * 0.6
        medium_value_volume = row['total_volume'] * 0.25
        regular_value_volume = row['total_volume'] * 0.15
        
        monthly_data.append({
            'transaction_date': row['transaction_date'],
            'bank_id': row['bank_id'],
            'bank_name': row['bank_name'],
            'high_value_customers': high_value_customers,
            'medium_value_customers': medium_value_customers,
            'regular_customers': regular_customers,
            'high_value_volume': round(high_value_volume, 2),
            'medium_value_volume': round(medium_value_volume, 2),
            'regular_value_volume': round(regular_value_volume, 2),
            'avg_high_value': round(high_value_volume / high_value_customers if high_value_customers > 0 else 0, 2),
            'avg_medium_value': round(medium_value_volume / medium_value_customers if medium_value_customers > 0 else 0, 2),
            'avg_regular_value': round(regular_value_volume / regular_customers if regular_customers > 0 else 0, 2),
            'customer_retention_rate': round(np.random.uniform(85, 98), 1),
            'new_customers': int(row['unique_customers'] * np.random.uniform(0.05, 0.15))
        })
    
    return pd.DataFrame(monthly_data)

def create_powerbi_datasets():
    """
    Create all datasets needed for PowerBI dashboard
    """
    print("Generating comprehensive banking data...")
    main_data = generate_comprehensive_banking_data()
    
    print("Generating anomaly detection data...")
    anomaly_data = generate_anomaly_data()
    
    print("Generating customer analysis data...")
    customer_data = generate_customer_analysis_data()
    
    # Create summary tables for PowerBI
    print("Creating summary tables...")
    
    # Bank summary
    bank_summary = main_data.groupby(['bank_id', 'bank_name', 'market_segment']).agg({
        'total_volume': ['sum', 'mean', 'std'],
        'transaction_count': 'sum',
        'unique_customers': 'sum',
        'data_quality_score': 'mean'
    }).round(2)
    
    bank_summary.columns = ['total_lifetime_volume', 'avg_daily_volume', 'volume_volatility', 
                           'total_transactions', 'total_customers', 'avg_data_quality']
    bank_summary = bank_summary.reset_index()
    
    # Monthly trends
    monthly_summary = main_data.groupby(['year', 'month', 'month_name']).agg({
        'total_volume': 'sum',
        'transaction_count': 'sum',
        'unique_customers': 'sum',
        'bank_id': 'nunique'
    }).round(2)
    
    monthly_summary.columns = ['monthly_volume', 'monthly_transactions', 'monthly_customers', 'active_banks']
    monthly_summary = monthly_summary.reset_index()
    
    # Weekly patterns
    weekly_patterns = main_data.groupby(['day_name', 'day_of_week', 'is_weekend']).agg({
        'total_volume': 'mean',
        'transaction_count': 'mean',
        'unique_customers': 'mean'
    }).round(2)
    
    weekly_patterns.columns = ['avg_daily_volume', 'avg_daily_transactions', 'avg_daily_customers']
    weekly_patterns = weekly_patterns.reset_index()
    
    return {
        'daily_transactions': main_data,
        'anomaly_analysis': anomaly_data,
        'customer_analysis': customer_data,
        'bank_summary': bank_summary,
        'monthly_trends': monthly_summary,
        'weekly_patterns': weekly_patterns
    }

def save_datasets_for_powerbi(output_dir='./powerbi_data/'):
    """
    Save all datasets as CSV files for PowerBI import
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    datasets = create_powerbi_datasets()
    
    for dataset_name, df in datasets.items():
        file_path = f"{output_dir}{dataset_name}.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved {len(df)} records to {file_path}")
    
    # Create metadata file
    metadata = {
        'generated_at': datetime.now().isoformat(),
        'datasets': {
            name: {
                'records': len(df),
                'columns': list(df.columns),
                'file_size_mb': round(len(df) * len(df.columns) * 8 / (1024*1024), 2)
            }
            for name, df in datasets.items()
        },
        'total_records': sum(len(df) for df in datasets.values()),
        'powerbi_connection_notes': {
            'data_source': 'CSV files',
            'refresh_frequency': 'Daily',
            'key_measures': [
                'Total Volume',
                'Transaction Count',
                'Average Transaction Value',
                'Unique Customers',
                'Data Quality Score'
            ],
            'key_dimensions': [
                'Bank ID',
                'Transaction Date',
                'Bank Name',
                'Market Segment',
                'Day of Week',
                'Month'
            ]
        }
    }
    
    with open(f"{output_dir}dataset_metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\nDataset generation complete!")
    print(f"Total datasets: {len(datasets)}")
    print(f"Total records: {metadata['total_records']:,}")
    print(f"Files saved to: {output_dir}")
    
    return datasets

def generate_powerbi_dax_measures():
    """
    Generate DAX measures that would be useful in PowerBI
    """
    dax_measures = {
        'Total Volume': 'SUM(daily_transactions[total_volume])',
        'Average Transaction Value': 'DIVIDE(SUM(daily_transactions[total_volume]), SUM(daily_transactions[transaction_count]))',
        'Total Customers': 'SUM(daily_transactions[unique_customers])',
        'Volume Growth %': '''
        VAR CurrentPeriod = SUM(daily_transactions[total_volume])
        VAR PreviousPeriod = CALCULATE(
            SUM(daily_transactions[total_volume]),
            DATEADD(daily_transactions[transaction_date], -1, MONTH)
        )
        RETURN DIVIDE(CurrentPeriod - PreviousPeriod, PreviousPeriod)
        ''',
        'Top 5 Banks Filter': '''
        VAR Top5Banks = TOPN(5, 
            SUMMARIZE(daily_transactions, daily_transactions[bank_id], 
                "TotalVolume", SUM(daily_transactions[total_volume])),
            [TotalVolume], DESC
        )
        RETURN IF(daily_transactions[bank_id] IN VALUES(Top5Banks[bank_id]), 1, 0)
        ''',
        'Anomaly Detection': '''
        VAR CurrentVolume = SUM(daily_transactions[total_volume])
        VAR AvgVolume = CALCULATE(
            AVERAGE(daily_transactions[total_volume]),
            FILTER(ALL(daily_transactions), 
                daily_transactions[bank_id] = MAX(daily_transactions[bank_id])
            )
        )
        VAR StdDev = CALCULATE(
            STDEV.P(daily_transactions[total_volume]),
            FILTER(ALL(daily_transactions),
                daily_transactions[bank_id] = MAX(daily_transactions[bank_id])
            )
        )
        RETURN IF(ABS(CurrentVolume - AvgVolume) > 2 * StdDev, "Anomaly", "Normal")
        ''',
        'Data Quality Score': 'AVERAGE(daily_transactions[data_quality_score])',
        'Weekend Effect': '''
        VAR WeekendAvg = CALCULATE(
            AVERAGE(daily_transactions[total_volume]),
            daily_transactions[is_weekend] = TRUE()
        )
        VAR WeekdayAvg = CALCULATE(
            AVERAGE(daily_transactions[total_volume]),
            daily_transactions[is_weekend] = FALSE()
        )
        RETURN DIVIDE(WeekendAvg, WeekdayAvg) - 1
        ''',
        'Monthly Trend': '''
        VAR CurrentMonth = SUM(daily_transactions[total_volume])
        VAR LastMonth = CALCULATE(
            SUM(daily_transactions[total_volume]),
            DATEADD(daily_transactions[transaction_date], -1, MONTH)
        )
        RETURN CurrentMonth - LastMonth
        '''
    }
    
    # Save DAX measures to file
    with open('./powerbi_data/dax_measures.txt', 'w') as f:
        f.write("PowerBI DAX Measures for EFT Banking Dashboard\n")
        f.write("=" * 50 + "\n\n")
        
        for measure_name, dax_code in dax_measures.items():
            f.write(f"{measure_name}:\n")
            f.write(f"{dax_code}\n\n")
            f.write("-" * 30 + "\n\n")
    
    return dax_measures

if __name__ == "__main__":
    # Generate all data for PowerBI
    datasets = save_datasets_for_powerbi()
    
    # Generate DAX measures
    dax_measures = generate_powerbi_dax_measures()
    
    print("\nSample data for PowerBI dashboard has been generated!")
    print("Files created:")
    print("- daily_transactions.csv (main dataset)")
    print("- anomaly_analysis.csv (anomaly detection)")
    print("- customer_analysis.csv (customer segments)")
    print("- bank_summary.csv (bank performance)")
    print("- monthly_trends.csv (time series)")
    print("- weekly_patterns.csv (weekly patterns)")
    print("- dataset_metadata.json (metadata)")
    print("- dax_measures.txt (PowerBI measures)")
    
    # Display sample of main dataset
    print(f"\nSample of daily_transactions data:")
    print(datasets['daily_transactions'].head())
    
    print(f"\nDataset shapes:")
    for name, df in datasets.items():
        print(f"- {name}: {df.shape}")