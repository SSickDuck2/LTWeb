#!/usr/bin/env python3
"""
Fix missing subjects for curricula by extracting from stored curriculum_curriculum_subjects data
"""
import sqlite3
import json

def fix_subjects():
    conn = sqlite3.connect('database/syllabus.db', timeout=30.0)
    cur = conn.cursor()
    
    # Get all curricula that have curriculum_curriculum_subjects but no subjects stored
    cur.execute('''
    SELECT c.id, c.attributes
    FROM curricula c
    LEFT JOIN subjects s ON c.id = s.curricula_id
    WHERE s.id IS NULL
    ''')
    
    curricula_to_fix = cur.fetchall()
    print(f"Found {len(curricula_to_fix)} curricula without subjects in DB")
    
    total_inserted = 0
    for curr_id, attrs_json in curricula_to_fix:
        try:
            attrs = json.loads(attrs_json)
            
            # Extract curriculum_curriculum_subjects data
            subj_data = attrs.get('curriculum_curriculum_subjects', {}).get('data', [])
            if not subj_data:
                continue
            
            # Convert to list if it's a dict
            if isinstance(subj_data, dict):
                subj_data = [subj_data]
            
            # From each curriculum_curriculum_subjects item, extract the actual subject
            subjects = []
            for subj_link in subj_data:
                if not isinstance(subj_link, dict):
                    continue
                subj_attrs = subj_link.get('attributes', {})
                subj_rel = subj_attrs.get('curriculum_subject', {}).get('data')
                if subj_rel:
                    subjects.append(subj_rel)
            
            if subjects:
                # Insert subjects into DB
                for subj in subjects:
                    subj_id = subj.get('id')
                    subj_attrs = subj.get('attributes', {})
                    subj_attrs_json = json.dumps(subj_attrs, ensure_ascii=False)
                    subj_raw_json = json.dumps(subj, ensure_ascii=False)
                    
                    cur.execute(
                        'INSERT OR REPLACE INTO subjects (id, curricula_id, attributes, raw) VALUES (?, ?, ?, ?)',
                        (subj_id, curr_id, subj_attrs_json, subj_raw_json)
                    )
                    total_inserted += 1
                
                if total_inserted % 100 == 0:
                    print(f"  Inserted {total_inserted} subjects so far...")
        except Exception as e:
            print(f"Error processing curriculum {curr_id}: {e}")
    
    conn.commit()
    conn.close()
    print(f"Total subjects inserted: {total_inserted}")

if __name__ == '__main__':
    fix_subjects()
