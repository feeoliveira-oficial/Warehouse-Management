import pandas as pd
from sqlalchemy import create_engine, text

# Function to connect to the SQL Server
def get_db_connection():
    server = 'FELIPE_LAPTOP\\SQL2019Express'
    database = 'warehouse_management'
    # Connection string using SQLAlchemy
    engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
    return engine

# Function to get sales and stock data from the database
def get_raw_data(conn):
    #Query for sales data
    sales_query = """
    SELECT product_id, quantity_sold, sale_date
    FROM sales_history
    """
    sales_data = pd.read_sql(sales_query, conn)
    
    # Query for reflected stock data
    stock_query = """
    SELECT product_id, stock_quantity AS quantity_in_stock, last_updated AS stock_date
    FROM stock
    """
    stock_data = pd.read_sql(stock_query, conn)
    
    return sales_data, stock_data

# Function to clean and process sales data
def clean_sales_data(sales_data):
    # Convert sale_date to datetime
    sales_data['sale_date'] = pd.to_datetime(sales_data['sale_date'])

    # Drop duplicates (if any)
    sales_data.drop_duplicates(inplace=True)

    # Check for and handle missing values
    sales_data.fillna(0, inplace=True)

    # Aggregate sales by product and date (if needed)
    sales_data = sales_data.groupby(['product_id', 'sale_date']).agg({'quantity_sold': 'sum'}).reset_index()

    return sales_data

# Function to clean and process stock data
def clean_stock_data(stock_data):
    # Convert stock_date to datetime
    stock_data['stock_date'] = pd.to_datetime(stock_data['stock_date'])

    # Drop duplicates (if any)
    stock_data.drop_duplicates(inplace=True)

    # Check for and handle missing values
    stock_data.fillna(0, inplace=True)

    # Aggregate stock by product and date (if needed)
    stock_data = stock_data.groupby(['product_id', 'stock_date']).agg({'quantity_in_stock': 'sum'}).reset_index()

    return stock_data

# Function to merge sales and stock data for further analysis
def merge_sales_and_stock(sales_data, stock_data):
    # Merge the two datasets on product_id and date (joining on nearest date match)
    merged_data = pd.merge_asof(
        sales_data.sort_values('sale_date'), 
        stock_data.sort_values('stock_date'), 
        left_on='sale_date', 
        right_on='stock_date', 
        by='product_id', 
        direction='backward'
    )

    return merged_data

if __name__ == "__main__":
    # Step 1: Connect to the database
    engine = get_db_connection()

    with engine.connect() as conn:
        # Step 2: Get raw sales and stock data
        sales_data, stock_data = get_raw_data(conn)

        # Step 3: Clean and process sales data
        cleaned_sales_data = clean_sales_data(sales_data)
        print("Cleaned Sales Data:")
        print(cleaned_sales_data.head())

        # Step 4: Clean and process stock data
        cleaned_stock_data = clean_stock_data(stock_data)
        print("Cleaned Stock Data:")
        print(cleaned_stock_data.head())

        # Step 5: Merge sales and stock data for further analysis
        merged_data = merge_sales_and_stock(cleaned_sales_data, cleaned_stock_data)
        print("Merged Sales and Stock Data:")
        print(merged_data.head())

    # save the cleaned and merged data as a CSV for further use if needed
    cleaned_sales_data.to_csv('cleaned_sales_data.csv', index=False)
    cleaned_stock_data.to_csv('cleaned_stock_data.csv', index=False)
    merged_data.to_csv('merged_data.csv', index=False)
    
    print("Data saved as CSV files: 'cleaned_sales_data.csv', 'cleaned_stock_data.csv', 'merged_data.csv'")
