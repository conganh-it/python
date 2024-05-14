import requests
import pyodbc
import datetime
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL của dịch vụ API thời tiết
url = 'http://127.0.0.1:8000/weather'

# Chuỗi kết nối tới SQL Server
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=CONGANH\\SQLEXPRESS;'  # Lưu ý cần hai dấu gạch chéo ngược
    'DATABASE=abc;'
    'UID=sa;'
    'PWD=abc1234'
)

# Kết nối tới SQL Server
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Tạo bảng nếu chưa tồn tại
def create_table_if_not_exists(cursor):
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='a' AND xtype='U')
    CREATE TABLE WeatherData (
        Date DATE,
        Temperature FLOAT,
        Humidity INT,
        Pressure INT,
        City NVARCHAR(50)
    )
    """
    cursor.execute(create_table_query)

create_table_if_not_exists(cursor)

# Câu lệnh SQL để chèn dữ liệu
insert_query = """
INSERT INTO a (Date, Temperature, Humidity, Pressure, City)
VALUES (?, ?, ?, ?, ?)
"""

# Lấy dữ liệu từ URL
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
else:
    logger.error(f"Failed to fetch data from URL: {url}")
    data = []

# Chèn dữ liệu vào bảng
for item in data:
    logger.info(f"Processing item: {item}")
    temperature = item.get('Temperature')
    city = item.get('City')
    date_str = item.get('Date')
    humi = item.get('Humidity')
    press = item.get('Pressure')

    try:
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError as e:
        logger.error(f"Date format error for item {item}: {e}")
        continue

    if temperature is not None and city is not None and date is not None:
        logger.info(f"Inserting record: Temperature={temperature}, City={city}, Date={date}")
        cursor.execute(insert_query, (date, temperature, humi, press, city))
    else:
        logger.warning(f"Invalid record skipped: {item}")

# Commit các thay đổi
conn.commit()

# Đóng kết nối
cursor.close()
conn.close()
