# salesHistory.py

import pyodbc

# Function to insert a sale into the sales_history table
def insert_sale(conn, product_id, quantity_sold, sale_date):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sales_history (product_id, quantity_sold, sale_date)
            VALUES (?, ?, ?)
        """, (product_id, quantity_sold, sale_date))
        conn.commit()
        print(f"Sale for product_id '{product_id}' inserted successfully.")
    except Exception as e:
        print(f"Failed to insert sale: {e}")

# Function to retrieve all sales from the sales_history table
def get_all_sales(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sales_history")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch sales history: {e}")

# Function to get sales by a specific product
def get_sales_by_product(conn, product_id):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM sales_history WHERE product_id = ?
        """, (product_id,))
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch sales for product_id '{product_id}': {e}")
