import sqlite3
import json

conn = sqlite3.connect('database/syllabus.db')
cur = conn.cursor()

# Check schools with faculties
cur.execute('SELECT school_id, COUNT(*) as count FROM faculties WHERE school_id IS NOT NULL GROUP BY school_id ORDER BY count DESC')
rows = cur.fetchall()
print('Schools with faculties:')
for row in rows:
    print(f'  School ID: {row[0]}, Count: {row[1]}')

# Check specific school 25
cur.execute('SELECT COUNT(*) FROM faculties WHERE school_id = 25')
count = cur.fetchone()[0]
print(f'\nFaculties for school 25: {count}')

if count > 0:
    cur.execute('SELECT id, attributes FROM faculties WHERE school_id = 25 LIMIT 3')
    rows = cur.fetchall()
    for row in rows:
        attrs = json.loads(row[1])
        print(f'  ID: {row[0]}, Name: {attrs["name"]}')

conn.close()