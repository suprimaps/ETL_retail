import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging

def transform_data(df):
    # Convert 'date' column to datetime (handle bad/missing values)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Fill missing values
    df['price'] = df['price'].fillna(df.groupby('product_id')['price'].transform('median'))
    df['quantity'] = df['quantity'].fillna(0)

    # Extract sale year
    df['sale_year'] = df['date'].dt.year

    # Map country to country code (only if column exists)
    country_map = {'USA': 'US', 'UK': 'GB', 'Japan': 'JP'}
    df['country_code'] = df['country'].map(country_map).fillna('Unknown')
    print("Data transformation complete.")
    return df

def load_data_to_warehouse(df, table_name):
    # Load DB credentials from environment variables (or use defaults)
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'root')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_name = 'retail_warehouse'

    # Create PostgreSQL connection using SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')

    # Load dataframe into PostgreSQL table
    try: 
        max_date= pd.read_sql(f'Select MAX(date) from {table_name}',engine).iloc[0,0]
        new_data = df[df['date'] > max_date] if max_date else df
        ##new_data.to_sql(table_name, con = engine, if_exists='append', index=False, chunksize= 10000)
        new_data.to_sql(table_name, con = engine, if_exists='append', index=False, chunksize= 10000)
        print(f"Data loaded to '{table_name}' table in the warehouse.")
    except Exception as e: 
        print(f'Error loading data to the warehouse: {e}')
        return

if __name__ == "__main__":
    # Step 1: Load CSV with correct separator and whitespace handling
    df = pd.read_csv('retailData.csv', header=0, sep=',', skipinitialspace=True,parse_dates=['date'])

    # Step 2: Debug check
    print("Columns:", df.columns.tolist())
    print("Preview:\n", df.head())

    # Step 3: Transform data
    transformed_df = transform_data(df)

    # Step 4: Load to warehouse
    load_data_to_warehouse(transformed_df, 'sales')

    logging.basicConfig(filename='etl.log', level=logging.INFO)

    logging.info(f"ETL completed at {pd.Timestamp.now()}")

    print("🎉 ETL process completed successfully.")
