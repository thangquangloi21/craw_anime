import sqlite3

conn = sqlite3.connect('mydatabase.db')
cursor = conn.cursor()

# Thêm một bản ghi vào bảng employees
cursor.execute("INSERT INTO employees (id, name, age) VALUES (1, 'John Doe', 25)")

# Thêm nhiều bản ghi vào bảng employees
employees = [
    (2, 'Jane Smith', 30),
    (3, 'Bob Johnson', 35),
    (4, 'Alice Williams', 28)
]
cursor.executemany("INSERT INTO employees (id, name, age) VALUES (?, ?, ?)", employees)

# Lưu các thay đổi vào cơ sở dữ liệu
conn.commit()

# Đóng kết nối với cơ sở dữ liệu
conn.close()
