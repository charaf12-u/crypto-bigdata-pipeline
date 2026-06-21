import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)

cursor = conn.cursor()

cursor.execute("""
SELECT CURRENT_ACCOUNT(),
       CURRENT_USER(),
       CURRENT_ROLE(),
       CURRENT_WAREHOUSE();
""")

print(cursor.fetchone())

cursor.close()
conn.close()