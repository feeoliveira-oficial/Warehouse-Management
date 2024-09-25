# stock.py

import pyodbc

# Function to insert or update stock for a product
def upsert_stock(conn, product_id, stock_quantity):
    try:
        cursor = conn.cursor()
        # Check if product exists in the stock table
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM stock WHERE product_id = ?)
                UPDATE stock
                SET stock_quantity = ?, last_updated = GETDATE()
                WHERE product_id = ?
            ELSE
                INSERT INTO stock (product_id, stock_quantity, last_updated)
                VALUES (?, ?, GETDATE())
        """, (product_id, stock_quantity, product_id, product_id, stock_quantity))
        conn.commit()
        print(f"Stock for product_id '{product_id}' upserted successfully.")
    except Exception as e:
        print(f"Failed to upsert stock: {e}")

# Function to retrieve stock details for all products
def get_stock(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stock")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch stock data: {e}")

# Function to reduce stock when a sale is made
def reduce_stock(conn, product_id, quantity_sold):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE stock
            SET stock_quantity = stock_quantity - ?
            WHERE product_id = ? AND stock_quantity >= ?
        """, (quantity_sold, product_id, quantity_sold))
        if cursor.rowcount == 0:
            print(f"Insufficient stock for product_id '{product_id}'.")
        else:
            conn.commit()
            print(f"Stock reduced by {quantity_sold} for product_id '{product_id}'.")
    except Exception as e:
        print(f"Failed to reduce stock: {e}")
        

