import time
import schedule
from demand_prediction import (
    get_db_connection,
    get_sales_data,
    prepare_data,
    train_and_predict,
    forecast_sales,
    insert_forecasts_to_db
)

# Parameters for the automation
PRODUCT_ID = 1  # Adjust with the appropriate product_id
FORECAST_PERIOD = 30  # Days to forecast

# Function to automate the forecasting process
def run_forecast():
    print("Starting forecast automation...")

    # Step 1: Connect to the database
    engine = get_db_connection()

    with engine.connect() as conn:
        # Step 2: Get and prepare the sales data
        sales_data = get_sales_data(conn)
        X, y = prepare_data(sales_data)

        # Step 3: Train the model and make predictions
        model = train_and_predict(X, y)
        forecast_df = forecast_sales(model, FORECAST_PERIOD, PRODUCT_ID)

        # Step 4: Insert forecasts into the database
        insert_forecasts_to_db(conn, forecast_df, PRODUCT_ID)

    print("Forecast completed and inserted into the database.")

# Function to set up the schedule
def setup_schedule():
    # Schedule the forecast to run once a day at a specified time
    schedule.every().day.at("00:00").do(run_forecast)  # Set your preferred time
    
    print("Scheduler is set up to run forecast at 00:00 every day.")

    while True:
        # Run all scheduled tasks
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    setup_schedule()
