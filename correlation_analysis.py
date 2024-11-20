import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

db_config = {
    "dbname": "your_database_name",
    "user": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port"
}

try:
    conn = psycopg2.connect(**db_config)
    print("Koneksi berhasil!")
except Exception as e:
    print(f"Koneksi gagal: {e}")

# plih ticker dan jenis minyak yang dipilih
selected_ticker = "2222.SR"
selected_oil_type = "Crude Oil Brent"

query = f"""
WITH stock_filtered AS (
    SELECT
        stock_change,
        datetime AS stock_datetime,
        ROW_NUMBER() OVER (ORDER BY datetime) AS rn
    FROM company_stocks
    WHERE ticker = '{selected_ticker}'
),
oil_filtered AS (
    SELECT
        price_change,
        datetime AS oil_datetime,
        ROW_NUMBER() OVER (ORDER BY datetime) AS rn
    FROM oil_prices
    WHERE type = '{selected_oil_type}'
)
SELECT
    sf.stock_change,
    sf.stock_datetime,
    of.price_change,
    of.oil_datetime
FROM
    stock_filtered sf
JOIN
    oil_filtered of
ON
    sf.rn = of.rn
"""

try:
    data = pd.read_sql_query(query, conn)
    print("Data berhasil diambil!")
except Exception as e:
    print(f"Terjadi kesalahan saat mengambil data: {e}")
    data = pd.DataFrame()

if not data.empty:
    print("Data yang diambil:")
    print(data.head())

    plt.figure(figsize=(10, 6))
    plt.plot(data['stock_datetime'], data['stock_change'], label="Stock Change", color="blue", marker="o", linestyle="-")
    plt.plot(data['oil_datetime'], data['price_change'], label="Oil Price Change", color="orange", marker="o", linestyle="--")
    plt.title("Original Price Trend: Stock vs Oil", fontsize=14)
    plt.xlabel("Datetime", fontsize=12)
    plt.ylabel("Price Change", fontsize=12)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

    stock_mean = data['stock_change'].mean()
    stock_std = data['stock_change'].std()
    oil_mean = data['price_change'].mean()
    oil_std = data['price_change'].std()

    data['stock_change_normalized'] = (data['stock_change'] - stock_mean) / stock_std
    data['price_change_normalized'] = (data['price_change'] - oil_mean) / oil_std

    plt.figure(figsize=(10, 6))
    plt.plot(data['stock_datetime'], data['stock_change_normalized'], label="Stock Change Normalized", color="green", marker="o", linestyle="-")
    plt.plot(data['oil_datetime'], data['price_change_normalized'], label="Oil Price Change Normalized", color="red", marker="o", linestyle="--")
    plt.title("Normalized Price Trend: Stock vs Oil", fontsize=14)
    plt.xlabel("Datetime", fontsize=12)
    plt.ylabel("Normalized Price Change (Z-Score)", fontsize=12)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

    correlation_original = data['stock_change'].corr(data['price_change'])
    correlation_normalized = data['stock_change_normalized'].corr(data['price_change_normalized'])

    plt.figure(figsize=(8, 6))
    plt.scatter(data['price_change_normalized'], data['stock_change_normalized'], alpha=0.7, color="purple")
    plt.title(f"Korelasi (Normalized) antara {selected_ticker} dan {selected_oil_type}", fontsize=14)
    plt.xlabel("Perubahan Harga Minyak (Normalized)", fontsize=12)
    plt.ylabel("Perubahan Harga Saham (Normalized)", fontsize=12)
    plt.grid()
    plt.tight_layout()
    plt.show()

    print(f"\nKorelasi sebelum normalisasi antara {selected_ticker} dan {selected_oil_type}: {correlation_original}")
    print(f"Korelasi setelah normalisasi antara {selected_ticker} dan {selected_oil_type}: {correlation_normalized}")
else:
    print("Tidak ada data untuk dianalisis.")

conn.close()
