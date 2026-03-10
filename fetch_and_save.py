#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

import requests


def parse_url_params(url: str):
    p = urlparse(url)
    base = f"{p.scheme}://{p.netloc}{p.path}"
    qs = parse_qs(p.query)
    # convert list values to single value strings
    params = {k: v[0] for k, v in qs.items()}
    return base, params


def fetch_all(session: requests.Session, url: str) -> List[Dict[str, Any]]:
    base, params = parse_url_params(url)
    page = 1
    results: List[Dict[str, Any]] = []
    while True:
        # Strapi v4 uses pagination[page]
        params_with_page = dict(params)
        params_with_page.setdefault('pagination[page]', page)
        resp = session.get(base, params=params_with_page, timeout=90)
        resp.raise_for_status()
        j = resp.json()
        data = j.get('data', j)
        if isinstance(data, list):
            results.extend(data)
        else:
            results.append(data)

        meta = j.get('meta') or {}
        pagination = meta.get('pagination') or {}
        if not pagination:
            break
        if pagination.get('page', 1) >= pagination.get('pageCount', 1):
            break
        page += 1
    return results


def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS schools (
            id INTEGER PRIMARY KEY,
            attributes TEXT,
            raw TEXT
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS faculties (
            id INTEGER PRIMARY KEY,
            school_id INTEGER,
            attributes TEXT,
            raw TEXT
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS majors (
            id INTEGER PRIMARY KEY,
            faculty_id INTEGER,
            attributes TEXT,
            raw TEXT
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS curricula (
            id INTEGER PRIMARY KEY,
            major_id INTEGER,
            attributes TEXT,
            raw TEXT
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS subjects (
            id INTEGER PRIMARY KEY,
            curricula_id INTEGER,
            attributes TEXT,
            raw TEXT
        )
        '''
    )
    # Create indexes for better search performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_faculties_school_id ON faculties (school_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_majors_faculty_id ON majors (faculty_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_curricula_major_id ON curricula (major_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_subjects_curricula_id ON subjects (curricula_id)")
    conn.commit()


def find_parent_id(attrs: Dict[str, Any], parent_key_fragment: str) -> Optional[int]:
    # Look for keys containing the fragment and extract id from `data`
    for k, v in attrs.items():
        if parent_key_fragment.lower() in k.lower():
            if isinstance(v, dict) and 'data' in v:
                d = v['data']
                if isinstance(d, dict) and 'id' in d:
                    return d['id']
                if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and 'id' in d[0]:
                    return d[0]['id']
            if isinstance(v, dict) and 'id' in v:
                return v['id']
    # fallback: try any relation-like dict with data->id
    for v in attrs.values():
        if isinstance(v, dict) and 'data' in v:
            d = v['data']
            if isinstance(d, dict) and 'id' in d:
                return d['id']
            if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and 'id' in d[0]:
                return d[0]['id']
    return None


def upsert(conn: sqlite3.Connection, table: str, row_id: int, attributes: Dict[str, Any], raw: Dict[str, Any], parent_col: Optional[str] = None, parent_id: Optional[int] = None):
    cur = conn.cursor()
    attrs_text = json.dumps(attributes, ensure_ascii=False)
    raw_text = json.dumps(raw, ensure_ascii=False)
    if parent_col and parent_id is not None:
        cur.execute(f"INSERT OR REPLACE INTO {table} (id, {parent_col}, attributes, raw) VALUES (?, ?, ?, ?)", (row_id, parent_id, attrs_text, raw_text))
    else:
        cur.execute(f"INSERT OR REPLACE INTO {table} (id, attributes, raw) VALUES (?, ?, ?)", (row_id, attrs_text, raw_text))
    conn.commit()


def process_and_store(conn: sqlite3.Connection, items: List[Dict[str, Any]], table: str, parent_fragment: Optional[str] = None, parent_col: Optional[str] = None):
    count = 0
    for item in items:
        item_id = item.get('id')
        attributes = item.get('attributes', {}) if isinstance(item, dict) else {}
        parent_id = None
        if parent_fragment and attributes:
            parent_id = find_parent_id(attributes, parent_fragment)
        upsert(conn, table, item_id, attributes, item, parent_col, parent_id)
        count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description='Fetch 3 NEU curriculum APIs and store into SQLite')
    parser.add_argument('--db', default='database/syllabus.db', help='SQLite DB path')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    endpoints = {
        'schools': 'https://courses.neu.edu.vn/backend/api/curriculum-schools?populate=curriculum_faculties&locale=vi',
        'faculties': 'https://courses.neu.edu.vn/backend/api/curriculum-faculties?populate=curriculum_majors&pagination[withCount]=false&pagination[pageSize]=50&locale=vi',
        'majors': 'https://courses.neu.edu.vn/backend/api/curriculum-majors?populate[curriculum_curricula][populate][curriculum_curriculum_subjects][populate][curriculum_subject]=*&pagination[withCount]=false&pagination[pageSize]=70&locale=vi',
    }

    session = requests.Session()
    try:
        conn = sqlite3.connect(args.db, timeout=30.0)
        conn.isolation_level = None  # autocommit mode
    except Exception as e:
        print('Could not open database:', e)
        print('Hint: Make sure DB Browser for SQLite is CLOSED.')
        sys.exit(1)

    ensure_tables(conn)

    print('Fetching schools...')
    schools = fetch_all(session, endpoints['schools'])
    n_schools = process_and_store(conn, schools, 'schools')
    print(f'Stored {n_schools} schools')

    # Process faculties from schools population
    total_fac = 0
    for school in schools:
        attrs = school.get('attributes', {})
        fac_field = None
        for k in attrs.keys():
            if 'facult' in k.lower():
                fac_field = k
                break
        if fac_field:
            fac_data = attrs[fac_field].get('data', [])
            if fac_data:
                # Add school_id to each faculty for relationship
                for fac in fac_data:
                    fac['school_id'] = school['id']
                s = process_and_store(conn, fac_data, 'faculties', parent_col='school_id')
                total_fac += s
    print(f'Stored {total_fac} faculties (from schools population)')

    print('Fetching additional faculties...')
    faculties = fetch_all(session, endpoints['faculties'])
    # Filter out faculties already stored
    existing_ids = set()
    cur = conn.cursor()
    cur.execute('SELECT id FROM faculties')
    existing_ids = {row[0] for row in cur.fetchall()}
    new_faculties = [f for f in faculties if f['id'] not in existing_ids]
    if new_faculties:
        n_fac = process_and_store(conn, new_faculties, 'faculties', parent_fragment='school', parent_col='school_id')
        print(f'Stored {n_fac} additional faculties')
    else:
        print('No additional faculties to store')

    print('Fetching majors...')
    majors = fetch_all(session, endpoints['majors'])
    n_maj = process_and_store(conn, majors, 'majors', parent_fragment='faculty', parent_col='faculty_id')
    print(f'Stored {n_maj} majors')

    # If majors include curricula populated, store them too
    total_curr = 0
    total_subj = 0
    for m in majors:
        attrs = m.get('attributes', {})
        curricula_field = None
        for k in attrs.keys():
            if 'curricula' in k.lower() or 'curriculum' in k.lower():
                curricula_field = k
                break
        if curricula_field:
            data = attrs.get(curricula_field, {}).get('data', [])
            if isinstance(data, dict):
                data = [data]
            if data:
                c = process_and_store(conn, data, 'curricula', parent_fragment='major', parent_col='major_id')
                total_curr += c
                # Now process subjects from curricula
                for curr in data:
                    curr_attrs = curr.get('attributes', {})
                    subj_field = None
                    for k in curr_attrs.keys():
                        if 'curriculum_subjects' in k.lower() or 'subjects' in k.lower():
                            subj_field = k
                            break
                    if subj_field:
                        subj_data = curr_attrs.get(subj_field, {}).get('data', [])
                        if isinstance(subj_data, dict):
                            subj_data = [subj_data]
                        if subj_data:
                            # Extract the actual subject from curriculum_subject
                            subjects = []
                            for subj_link in subj_data:
                                subj_attrs = subj_link.get('attributes', {})
                                subj_rel = subj_attrs.get('curriculum_subject', {}).get('data')
                                if subj_rel:
                                    subjects.append(subj_rel)
                            if subjects:
                                s = process_and_store(conn, subjects, 'subjects', parent_fragment='curricula', parent_col='curricula_id')
                                total_subj += s

    print(f'Stored {total_curr} curricula (from majors population)')
    print(f'Stored {total_subj} subjects (from curricula population)')


if __name__ == '__main__':
    main()
