import sqlite3
conn = sqlite3.connect('database/syllabus.db')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM majors WHERE faculty_id IS NOT NULL')
print('majors with faculty_id:', cur.fetchone()[0])
cur.execute('SELECT id, faculty_id FROM majors WHERE faculty_id IS NOT NULL LIMIT 10')
for row in cur.fetchall():
    print(row)

# check a specific faculty filter
fid = 209
cur.execute('SELECT COUNT(*) FROM majors WHERE faculty_id=?', (fid,))
print(f"majors for faculty {fid}:", cur.fetchone()[0])
cur.execute('SELECT id FROM majors WHERE faculty_id=? LIMIT 5', (fid,))
print(cur.fetchall())

conn.close()