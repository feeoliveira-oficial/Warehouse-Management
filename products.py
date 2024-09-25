# Function to insert a new product into the products table
def insert_product(conn, product_name, category, price):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO products (product_name, category, price)
            VALUES (?, ?, ?)
        """, (product_name, category, price))
        conn.commit()
        print(f"Product '{product_name}' inserted successfully.")
    except Exception as e:
        print(f"Failed to insert product: {e}")

# Function to query all products from the products table
def get_all_products(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM products")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Failed to fetch products: {e}")

# Function to update stock quantity in the stock table
def update_stock(conn, product_id, stock_quantity):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE stock
            SET stock_quantity = ?, last_updated = GETDATE()
            WHERE product_id = ?
        """, (stock_quantity, product_id))
        conn.commit()
        print(f"Stock for product_id '{product_id}' updated successfully.")
    except Exception as e:
        print(f"Failed to update stock: {e}")
