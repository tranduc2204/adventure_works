# AdventureWorks SQL Server & Data Ingestion

Dự án thiết lập môi trường SQL Server 2022 cục bộ bằng Docker và tự động nạp dữ liệu từ 8 file CSV vào database `AdventureWorks`.

---

## Cấu trúc thư mục (Clean Architecture)

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
├── .env.example             # Template cấu hình mẫu
├── .gitignore
├── docker-compose.yml
└── requirements.txt
```

---

## Hướng dẫn cài đặt & Chạy

### 1. Chuẩn bị môi trường
Tạo file `.env` từ file mẫu:
```bash
cp .env.example .env
```
*(Chỉnh sửa mật khẩu `SA_PASSWORD` nếu cần)*.

Cài đặt thư viện Python (nếu chạy local):
```bash
pip install -r requirements.txt
```

### 2. Khởi chạy toàn bộ hệ thống
Chạy script tự động hóa:
```bash
./scripts/run_sqlserver.sh
```

Hoặc sử dụng Docker Compose:
```bash
docker compose up -d
```
