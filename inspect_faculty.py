import sqlite3, json
conn=sqlite3.connect('database/syllabus.db')
cur=conn.cursor()
cur.execute('SELECT id, raw FROM faculties LIMIT 1')
row=cur.fetchone()
if row:
    print('ID',row[0])
    print(json.dumps(json.loads(row[1]), indent=2, ensure_ascii=False)[:2000])
conn.close()