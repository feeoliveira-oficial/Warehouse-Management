# testConnection.py

from connect import get_db_connection
from products import insert_product, get_all_products
from stock import upsert_stock, get_stock, reduce_stock
from salesHistory import insert_sale, get_all_sales, get_sales_by_product
from demand_forecast import insert_forecast, get_all_forecasts, get_forecast_by_product
from replenishment import insert_replenishment, get_all_replenishments, get_replenishment_by_product

if __name__ == "__main__":
    # Get the database connection
    conn = get_db_connection()

    if conn:
        # Insert a test product
        insert_product(conn, 'Leather Boots', 'Footwear', 149.99)
        
        # Insert stock for the product
        upsert_stock(conn, 1, 200)
        
        # Reduce stock for a sale
        reduce_stock(conn, 1, 20)

        # Insert a test sale
        insert_sale(conn, 1, 20, '2024-09-25')
        
        # Insert a forecast for the product
        insert_forecast(conn, 1, 100, '2024-10-01')

        # Insert a replenishment recommendation for the product
        insert_replenishment(conn, 1, 150, '2024-10-05')

        # Display all replenishments
        print("All replenishment recommendations in the database:")
        get_all_replenishments(conn)
        
        # Display replenishment for a specific product
        print("Replenishment for product_id 1:")
        get_replenishment_by_product(conn, 1)

        # Close the connection
        conn.close()
