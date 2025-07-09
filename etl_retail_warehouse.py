import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

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
    if 'country' in df.columns:
        df['country_code'] = df['country'].map(country_map).fillna('Unknown')
    else:
        df['country_code'] = 'Unknown'

    print("✅ Data transformation complete.")
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
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=10000)
    print(f"✅ Data loaded to '{table_name}' table.")

if __name__ == "__main__":
    # Step 1: Load CSV with correct separator and whitespace handling
    df = pd.read_csv('retailData.csv', header=0, sep=',', skipinitialspace=True)

    # Step 2: Debug check
    print("🔎 Columns:", df.columns.tolist())
    print("📄 Preview:\n", df.head())

    # Step 3: Transform data
    transformed_df = transform_data(df)

    # Step 4: Load to warehouse
    load_data_to_warehouse(transformed_df, 'sales')

    print("🎉 ETL process completed successfully.")
