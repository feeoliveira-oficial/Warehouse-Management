import pyodbc
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# Function to connect to the SQL Server
def get_db_connection():
    server = 'FELIPE_LAPTOP\\SQL2019Express'
    database = 'warehouse_management'
    # Connection string using SQLAlchemy
    engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
    return engine

# Function to get sales data from the database
def get_sales_data(conn):
    query = """
    SELECT product_id, quantity_sold, sale_date
    FROM sales_history
    """
    try:
        sales_data = pd.read_sql(query, conn)
        return sales_data
    except Exception as e:
        print(f"Failed to fetch sales data: {e}")
        return None

# Prepare data for regression model
def prepare_data(df):
    # Convert sale_date to datetime and extract relevant features
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['day'] = df['sale_date'].dt.day
    df['month'] = df['sale_date'].dt.month
    df['year'] = df['sale_date'].dt.year

    # Use day, month, and year as features for regression
    X = df[['day', 'month', 'year']]
    y = df['quantity_sold']

    return X, y

# Train the regression model and make predictions
def train_and_predict(X, y):
    # Split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = model.predict(X_test)

    # Calculate and print the model's error
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse}")

    return model

# Forecast future sales for a given period
def forecast_sales(model, forecast_period, product_id):
    # Generate date features for future predictions
    future_dates = pd.date_range(start=pd.to_datetime('today'), periods=forecast_period).to_frame(index=False, name='date')
    future_dates['day'] = future_dates['date'].dt.day
    future_dates['month'] = future_dates['date'].dt.month
    future_dates['year'] = future_dates['date'].dt.year
    
    X_future = future_dates[['day', 'month', 'year']]
    future_predictions = model.predict(X_future)

    # Show forecasted sales
    forecast_df = future_dates.copy()
    forecast_df['forecasted_sales'] = future_predictions
    print(forecast_df[['date', 'forecasted_sales']])
    
    # # Optional: Plot the forecast
    # plt.plot(forecast_df['date'], forecast_df['forecasted_sales'])
    # plt.xlabel('Date')
    # plt.ylabel('Forecasted Sales')
    # plt.title('Sales Forecast')
    # plt.show()

    return forecast_df

# Function to insert forecasted sales into the demand_forecast table
def insert_forecasts_to_db(conn, forecast_df, product_id):
    try:
        # iterate over each row of the forecast and insert into the database
        for _, row in forecast_df.iterrows():
            forecast_date = row['date'].date()
            forecasted_quantity = int(row['forecasted_sales'])
            
            # insert prediction into the demand_forecast table using named parameters
            insert_query = text("""
                INSERT INTO demand_forecast (product_id, forecasted_quantity, forecast_period, forecast_date)
                VALUES (:product_id, :forecasted_quantity, :forecast_period, GETDATE())
            """)
            
            # Use a dictionary to pass parameters as a single tuple
            conn.execute(insert_query, [
                {
                    'product_id': product_id,
                    'forecasted_quantity': forecasted_quantity,
                    'forecast_period': forecast_date
                }
            ])
        
        # Print success message
        print("Forecasts inserted into the database successfully.")
    except Exception as e:
        print(f"Failed to insert forecasts: {e}")


                

if __name__ == "__main__":
    # Connect to the database
    engine = get_db_connection()

    # Use a context manager to handle connection properly
    with engine.connect() as conn:
        # Get and prepare the sales data
        sales_data = get_sales_data(conn)
        X, y = prepare_data(sales_data)

        # Train the model and make predictions
        model = train_and_predict(X, y)

        # Specify the product ID for the forecasts (use the appropriate product ID)
        product_id = 1

        # Forecast sales for the next 30 days (for example)
        forecast_period = 30
        forecast_df = forecast_sales(model, forecast_period, product_id)

        # Insert the forecast into the demand_forecast table
        insert_forecasts_to_db(conn, forecast_df, product_id)
