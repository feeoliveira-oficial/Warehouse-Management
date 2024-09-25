import pyodbc

# Set your SQL Server connection details
server = 'FELIPE_LAPTOP\\SQL2019Express'  # Update with your server name
database = 'warehouse_management'  # Your database name
# For Windows Authentication, you don't need to provide username and password
# Set the connection string using Trusted_Connection for Windows Authentication
def get_db_connection():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};DATABASE={database};'
            f'Trusted_Connection=yes;'
        )
        print("Connection successful!")
        return conn
    except Exception as e:
        print(f"Connection failed: {e}")
        return None
