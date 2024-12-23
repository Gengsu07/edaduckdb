from connection import conn

result = conn.sql("SELECT * FROM db.public.ppmpkm LIMIT 10;").fetchall()
print(result)
