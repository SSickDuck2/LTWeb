#!/usr/bin/env python3
"""
Fix majors without curricula by checking all curricula and ensuring proper links
"""
import sqlite3
import json
import requests

def fix_majors_curricula():
    conn = sqlite3.connect('database/syllabus.db', timeout=30.0)
    cur = conn.cursor()
    
    # Get majors without curricula
    cur.execute('''
    SELECT m.id, m.faculty_id, m.attributes
    FROM majors m
    LEFT JOIN curricula c ON m.id = c.major_id
    WHERE c.id IS NULL
    ''')
    
    majors_to_fix = cur.fetchall()
    print(f"Found {len(majors_to_fix)} majors without curricula in DB")
    
    # For each major, check if it has curriculum_curricula data
    updated = 0
    for maj_id, fac_id, attrs_json in majors_to_fix:
        try:
            attrs = json.loads(attrs_json)
            curr_data = attrs.get('curriculum_curricula', {}).get('data', [])
            
            if not curr_data:
                print(f"Major {maj_id} has no curriculum_curricula data")
                continue
            
            # Convert to list if it's a dict
            if isinstance(curr_data, dict):
                curr_data = [curr_data]
            
            # Check each curriculum in the data
            for curr in curr_data:
                curr_id = curr.get('id')
                if curr_id:
                    # Update the major_id for this curriculum if it's 0 or NULL
                    cur.execute(
                        'UPDATE curricula SET major_id = ? WHERE id = ? AND (major_id IS NULL OR major_id = 0)',
                        (maj_id, curr_id)
                    )
                    if cur.rowcount > 0:
                        updated += 1
                        print(f"  Updated curriculum {curr_id} -> major {maj_id}")
        except Exception as e:
            print(f"Error processing major {maj_id}: {e}")
    
    conn.commit()
    
    # Now check again for majors without curricula  
    cur.execute('''
    SELECT COUNT(*)
    FROM majors m
    LEFT JOIN curricula c ON m.id = c.major_id
    WHERE c.id IS NULL
    ''')
    
    remaining = cur.fetchone()[0]
    print(f"\nUpdated {updated} curricula")
    print(f"Majors still without curricula: {remaining}")
    
    # Show which majors still don't have curricula
    if remaining > 0:
        cur.execute('''
        SELECT m.id, m.attributes
        FROM majors m
        LEFT JOIN curricula c ON m.id = c.major_id
        WHERE c.id IS NULL
        ''')
        rows = cur.fetchall()
        for maj_id, attrs_json in rows[:5]:
            attrs = json.loads(attrs_json)
            curr_data = attrs.get('curriculum_curricula', {}).get('data', [])
            print(f"Major {maj_id}: curriculum_curricula items = {len(curr_data) if curr_data else 0}")
    
    conn.close()

if __name__ == '__main__':
    fix_majors_curricula()
