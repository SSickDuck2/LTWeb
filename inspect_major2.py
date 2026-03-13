import sqlite3,json
conn=sqlite3.connect('database/syllabus.db')
cur=conn.cursor()
cur.execute('SELECT id, raw FROM majors LIMIT 5')
for row in cur.fetchall():
    print('MAJOR',row[0])
    print(json.dumps(json.loads(row[1]), indent=2, ensure_ascii=False)[:5000])
    print('---')
conn.close()