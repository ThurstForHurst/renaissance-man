from database.db import get_connection

conn = get_connection()
templates = conn.execute("SELECT id, name, time_available FROM routine_templates").fetchall()
conn.close()

for template in templates:
    print(f"{template['name']}: {template['time_available']}")