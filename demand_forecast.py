import pyodbc

# Function to insert a demand forecast for a product
def insert_forecast(conn, product_id, forecasted_quantity, forecast_period):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO demand_forecast (product_id, forecasted_quantity, forecast_period, forecast_date)
            VALUES (?, ?, ?, GETDATE())
        """, (product_id, forecasted_quantity, forecast_period))
        conn.commit()
        print(f"Forecast for product_id '{product_id}' inserted successfully.")
    except Exception as e:
        print(f"Failed to insert forecast: {e}")

# Function to retrieve all demand forecasts
def get_all_forecasts(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM demand_forecast")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch forecasts: {e}")

# Function to get demand forecasts by product
def get_forecast_by_product(conn, product_id):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM demand_forecast WHERE product_id = ?
        """, (product_id,))
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch forecast for product_id '{product_id}': {e}")
