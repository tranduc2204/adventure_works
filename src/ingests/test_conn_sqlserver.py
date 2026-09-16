import os
import urllib.parse
from dotenv import load_dotenv
import dlt
from dlt.sources.sql_database import sql_database

# 1. Đọc thông tin từ file .env
load_dotenv()

user = os.getenv("DB_USER", "sa")
password = urllib.parse.quote_plus(os.getenv("SA_PASSWORD", ""))
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "1433")
database = os.getenv("DB_NAME", "AdventureWorks")

connection_url = f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}"

print(f" Đang kiểm tra kết nối dlt tới SQL Server ({host}:{port})...")

try:
    # 2. Thử load metadata bảng customers qua dlt
    source = sql_database(
        credentials=connection_url,
        schema="dbo",
        table_names=["customers"]
    )
    print(" KẾT NỐI THÀNH CÔNG TỚI SQL SERVER!")
    print(f" dlt đã nhận diện được các bảng: {list(source.resources.keys())}")
except Exception as e:
    print(f"❌ KẾT NỐI THẤT BẠI: {e}")