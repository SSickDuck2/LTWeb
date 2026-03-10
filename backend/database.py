import sqlite3
from contextlib import contextmanager
from typing import Dict, Any, List, Optional
import json

DB_PATH = "database/syllabus.db"

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def get_table_data(table: str, id: Optional[int] = None, page: int = 1, page_size: int = 10, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with get_db() as conn:
        cur = conn.cursor()
        offset = (page - 1) * page_size
        where_clause = ""
        params = []
        if id:
            where_clause = "WHERE id = ?"
            params = [id]
        elif filters:
            conditions = []
            for k, v in filters.items():
                conditions.append(f"{k} = ?")
                params.append(v)
            where_clause = "WHERE " + " AND ".join(conditions)
        
        query = f"SELECT id, attributes, raw FROM {table} {where_clause} LIMIT ? OFFSET ?"
        params.extend([page_size, offset])
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Total count
        count_query = f"SELECT COUNT(*) FROM {table} {where_clause}"
        if id:
            count_params = [id]
        elif filters:
            count_params = list(filters.values())
        else:
            count_params = []
        cur.execute(count_query, count_params)
        total = cur.fetchone()[0]
        
        data = []
        for row in rows:
            data.append({
                "id": row[0],
                "attributes": json.loads(row[1]) if row[1] else {},
                "raw": json.loads(row[2]) if row[2] else {}
            })
        
        return {
            "data": data,
            "totalRecords": total,
            "page": page,
            "pageSize": page_size,
            "skippedRecords": offset
        }

def create_item(table: str, attributes: Dict[str, Any], parent_col: Optional[str] = None, parent_id: Optional[int] = None) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        raw = {"attributes": attributes}
        attrs_text = json.dumps(attributes, ensure_ascii=False)
        raw_text = json.dumps(raw, ensure_ascii=False)
        if parent_col and parent_id:
            cur.execute(f"INSERT INTO {table} ({parent_col}, attributes, raw) VALUES (?, ?, ?)", (parent_id, attrs_text, raw_text))
        else:
            cur.execute(f"INSERT INTO {table} (attributes, raw) VALUES (?, ?)", (attrs_text, raw_text))
        conn.commit()
        return cur.lastrowid

def update_item(table: str, id: int, attributes: Optional[Dict[str, Any]] = None) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        if attributes is not None:
            attrs_text = json.dumps(attributes, ensure_ascii=False)
            raw = {"attributes": attributes}
            raw_text = json.dumps(raw, ensure_ascii=False)
            cur.execute(f"UPDATE {table} SET attributes = ?, raw = ? WHERE id = ?", (attrs_text, raw_text, id))
        conn.commit()
        return cur.rowcount > 0

def delete_item(table: str, id: Optional[int] = None, ids: Optional[List[int]] = None) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        if id:
            cur.execute(f"DELETE FROM {table} WHERE id = ?", (id,))
        elif ids and ids:  # check not empty
            placeholders = ','.join('?' for _ in ids)
            cur.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", ids)
        else:
            return 0
        conn.commit()
        return cur.rowcount

def get_single_item(table: str, id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT id, attributes, raw FROM {table} WHERE id = ?", (id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "attributes": json.loads(row[1]) if row[1] else {},
                "raw": json.loads(row[2]) if row[2] else {}
            }
        return None