# replenishment.py

import pyodbc

# Function to insert a replenishment recommendation
def insert_replenishment(conn, product_id, recommended_quantity, suggested_date):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO replenishment (product_id, recommended_quantity, suggested_date)
            VALUES (?, ?, ?)""",
            (product_id, recommended_quantity, suggested_date))
        conn.commit()
        print(f"Replenishment recommendation for product_id '{product_id}' inserted successfully.")
    except Exception as e:
        print(f"Failed to insert replenishment recommendation: {e}")

# Function to retrieve all replenishment recommendations
def get_all_replenishments(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM replenishment")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch replenishment data: {e}")

# Function to retrieve replenishment recommendations by product
def get_replenishment_by_product(conn, product_id):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM replenishment WHERE product_id = ?
        """, (product_id,))
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch replenishment for product_id '{product_id}': {e}")
