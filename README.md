# AdventureWorks SQL Server & Data Ingestion

Dự án thiết lập môi trường SQL Server 2022 cục bộ bằng Docker và tự động hóa quy trình nạp dữ liệu (ETL/Data Ingestion) từ 8 file CSV vào database `AdventureWorks`.

---

## 1. Cấu trúc thư mục (Clean Architecture)

```
adventure_works/
├── data/                    # Chứa 8 file CSV nguồn
│   ├── calendar.csv
│   ├── customers.csv
│   ├── product_categories.csv
│   ├── product_subcategories.csv
│   ├── products.csv
│   ├── returns.csv
│   ├── sales.csv
│   └── territories.csv
├── docker/                  # Hạ tầng Docker
│   ├── Dockerfile
│   └── entrypoint.sh
├── scripts/                 # Scripts tự động hóa
│   └── run_sqlserver.sh
├── src/                     # Mã nguồn xử lý dữ liệu (Python)
│   └── ingest.py
├── .env                     # Biến môi trường local (KHÔNG push Git)
├── .env.example             # Template cấu hình mẫu cho Git
├── .gitignore
├── docker-compose.yml
└── requirements.txt
```

---

## 2. Chi tiết quy trình Ingestion (`src/ingest.py`)

Script [`src/ingest.py`](src/ingest.py) chịu trách nhiệm kết nối, tạo database và nạp dữ liệu từ các file CSV vào SQL Server:

### 2.1. Danh sách Dataset và Bảng ánh xạ

| File nguồn (`data/`) | Bảng đích SQL Server | Số dòng ước tính | Cột chuyển đổi kiểu ngày (`DATETIME`) |
| :--- | :--- | :---: | :--- |
| `calendar.csv` | `[calendar]` | 911 | `date` |
| `customers.csv` | `[customers]` | 18,148 | `birth_date` |
| `product_categories.csv` | `[product_categories]` | 4 | - |
| `product_subcategories.csv` | `[product_subcategories]` | 37 | - |
| `products.csv` | `[products]` | 293 | - |
| `returns.csv` | `[returns]` | 1,809 | `return_date` |
| `sales.csv` | `[sales]` | 23,935 | `order_date`, `stock_date` |
| `territories.csv` | `[territories]` | 10 | - |

---

### 2.2. Các bước xử lý trong Pipeline Ingestion

1. **Khởi động & Kiểm tra kết nối (`wait_for_sql_server`)**:
   - Sử dụng thư viện `pymssql` và `SQLAlchemy` thăm dò kết nối đến SQL Server (tối đa 30 lần, giãn cách 2 giây).
   - Đảm bảo SQL Server đã sẵn sàng nhận kết nối trước khi thực thi các bước tiếp theo.
2. **Khởi tạo Database (`ensure_database`)**:
   - Kết nối vào database hệ thống `master`.
   - Kiểm tra database `AdventureWorks` trong `sys.databases`. Nếu chưa có, tự động thực thi `CREATE DATABASE [AdventureWorks]`.
3. **Đọc và làm sạch dữ liệu**:
   - Quét toàn bộ file `.csv` trong thư mục `data/`.
   - Chuẩn hóa tên cột: xóa khoảng trắng thừa và chuyển về chữ thường (`lowercase`).
   - Tự động nhận diện và ép kiểu các trường ngày tháng (`DATE_COLUMNS`) sang định dạng chuẩn `datetime` để SQL Server tạo cột `DATETIME/DATE` thay vì `VARCHAR`.
4. **Nạp dữ liệu theo Batch (`to_sql`)**:
   - Ghi dữ liệu vào SQL Server theo từng khối (`chunksize=1000` dòng/lần) giúp tối ưu hiệu năng và tránh tràn bộ nhớ.
5. **Kiểm tra và Báo cáo (`verification`)**:
   - Chạy truy vấn `SELECT COUNT(*)` đếm số dòng thực tế trong từng bảng và in ra bảng tổng kết.

---

### 2.3. Các tham số cấu hình của `ingest.py`

Script tự động nạp cấu hình từ file `.env`. Bạn cũng có thể truyền tham số tùy chỉnh qua dòng lệnh:

| Tham số CLI | Biến môi trường tương ứng | Giá trị mặc định | Mô tả |
| :--- | :--- | :--- | :--- |
| `--host` | `DB_HOST` | `localhost` | Địa chỉ máy chủ SQL Server |
| `--port` | `DB_PORT` | `1433` | Cổng kết nối SQL Server |
| `--user` | `DB_USER` | `sa` | Tài khoản quản trị SQL Server |
| `--password` | `SA_PASSWORD` / `DB_PASSWORD` | - | Mật khẩu (đọc an toàn từ `.env`) |
| `--database` | `DB_NAME` | `AdventureWorks` | Tên database đích |
| `--data-dir` | `DATA_DIR` | `./data` | Đường dẫn thư mục chứa 8 file CSV |
| `--chunksize` | - | `1000` | Số lượng bản ghi nạp mỗi lượt |

---

## 3. Hướng dẫn cài đặt & Khởi chạy

### Bước 1: Thiết lập cấu hình môi trường
Copy file mẫu sang file `.env`:
```bash
cp .env.example .env
```
Mở file `.env` và đặt mật khẩu mong muốn tại dòng `SA_PASSWORD`:
```env
SA_PASSWORD=YourStrongPassword123!
```

---

### Bước 2: Khởi chạy Ingestion

Bạn có thể lựa chọn 1 trong 3 cách sau:

#### Cách 1: Tự động hóa toàn diện bằng Bash script (Khuyên dùng)
```bash
./scripts/run_sqlserver.sh
```
*Script sẽ tự động: Xóa container cũ &rarr; Build image &rarr; Khởi động SQL Server &rarr; Copy data &rarr; Chờ SQL Server sẵn sàng &rarr; Chạy Ingestion.*

#### Cách 2: Chạy trực tiếp `ingest.py` từ máy Host
(Dành cho trường hợp container SQL Server đã đang chạy):
```bash
# Kích hoạt venv (nếu có)
source venv/bin/activate

# Cài đặt thư viện
pip install -r requirements.txt

# Chạy ingestion (tự động đọc thông tin từ .env)
python src/ingest.py
```

#### Cách 3: Chạy Ingestion bên trong Docker container
```bash
docker exec -it sqlserver_adventureworks python3 /usr/src/app/ingest.py
```

---

## 5. Cấu hình Change Data Capture (CDC)

Dự án đã được kích hoạt tính năng **SQL Server Change Data Capture (CDC)** cho toàn bộ 8 bảng:

### 5.1. Bảng Change Table tương ứng

| Bảng nguồn | Primary Key | Trạng thái CDC | Bảng ghi nhận thay đổi (Change Table) |
| :--- | :--- | :---: | :--- |
| `calendar` | `[date]` | **Active** | `cdc.dbo_calendar_CT` |
| `customers` | `[customer_key]` | **Active** | `cdc.dbo_customers_CT` |
| `product_categories` | `[product_category_key]` | **Active** | `cdc.dbo_product_categories_CT` |
| `product_subcategories` | `[product_subcategory_key]` | **Active** | `cdc.dbo_product_subcategories_CT` |
| `products` | `[product_key]` | **Active** | `cdc.dbo_products_CT` |
| `returns` | `[return_date]`, `[territory_key]`, `[product_key]` | **Active** | `cdc.dbo_returns_CT` |
| `sales` | `[order_number]`, `[order_line_item]` | **Active** | `cdc.dbo_sales_CT` |
| `territories` | `[sales_territory_key]` | **Active** | `cdc.dbo_territories_CT` |

### 5.2. Các mã định danh thao tác CDC (`__$operation`)
- `1`: **DELETE** (Bản ghi bị xóa)
- `2`: **INSERT** (Bản ghi mới được thêm)
- `3`: **UPDATE (Before)** (Giá trị cũ trước khi sửa)
- `4`: **UPDATE (After)** (Giá trị mới sau khi sửa)

### 5.3. Kích hoạt lại CDC bất kỳ lúc nào
```bash
./venv/bin/python src/enable_cdc.py
```

- **Password**: *(Giá trị bạn đã đặt trong `.env`)*
- **Database**: `AdventureWorks`
- **Trust Server Certificate**: `True` (nếu dùng DBeaver / Azure Data Studio)
