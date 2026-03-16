#!/usr/bin/env python3
"""
Update attribute_en and raw_en columns on Supabase using data from syllabus_en.db.

Usage:
    python update_en_columns.py [--sqlite-db syllabus_en.db] [--supabase-db-url <url>]
"""
import argparse
import json
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg
from psycopg.types.json import Jsonb

# ---------------------------------------------------------------------------
# Reuse env/connection helpers from migrate_to_supabase
# ---------------------------------------------------------------------------

def load_env_file(env_path: str = ".env") -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ[key] = value


def _first_non_empty_env(keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return None


def _parse_conninfo_kv(text: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for token in text.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip().lower()
        value = value.strip().strip('"').strip("'")
        if key and value:
            result[key] = value
    return result


def _quote_conninfo_value(value: str) -> str:
    needs_quotes = any(ch.isspace() or ch in "'\\" for ch in value)
    if not needs_quotes:
        return value
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _build_conninfo_from_parts(seed: Optional[Dict[str, str]] = None) -> Optional[str]:
    parts = dict(seed or {})

    mapping = {
        "user": ["SUPABASE_DB_USER", "SUPABASE_USER", "PGUSER", "user"],
        "password": ["SUPABASE_DB_PASSWORD", "SUPABASE_PASSWORD", "PGPASSWORD", "password"],
        "host": ["SUPABASE_DB_HOST", "SUPABASE_HOST", "PGHOST", "host"],
        "port": ["SUPABASE_DB_PORT", "SUPABASE_PORT", "PGPORT", "port"],
        "dbname": ["SUPABASE_DB_NAME", "SUPABASE_DB_DBNAME", "PGDATABASE", "dbname"],
        "sslmode": ["SUPABASE_DB_SSLMODE", "PGSSLMODE", "sslmode"],
    }
    for field, env_keys in mapping.items():
        if field not in parts:
            val = _first_non_empty_env(env_keys)
            if val:
                parts[field] = val

    if not parts.get("user") or not parts.get("host"):
        return None

    parts.setdefault("port", "5432")
    parts.setdefault("dbname", "postgres")

    ordered_keys = ["user", "password", "host", "port", "dbname", "sslmode"]
    chunks = []
    for key in ordered_keys:
        value = parts.get(key)
        if value:
            chunks.append(f"{key}={_quote_conninfo_value(value)}")
    return " ".join(chunks)


def resolve_supabase_url() -> Optional[str]:
    direct = _first_non_empty_env(["SUPABASE_DB_URL", "SUPABASE_URL", "SUPABSE_URL"])
    if direct and "://" in direct:
        return direct
    seed: Dict[str, str] = {}
    if direct and "=" in direct:
        seed = _parse_conninfo_kv(direct)
    return _build_conninfo_from_parts(seed)


def normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def parse_json_text(value: Optional[str]) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except json.JSONDecodeError:
        return {}


def to_jsonb(value: Optional[str]) -> Jsonb:
    return Jsonb(parse_json_text(value))


def parse_pg_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return parse_json_text(value)
    return {}


def normalize_key_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower()


def find_key_token_from_dicts(dicts: Sequence[Dict[str, Any]], fields: Sequence[str]) -> Optional[str]:
    for field in fields:
        for data in dicts:
            value = normalize_key_value(data.get(field))
            if value:
                return f"{field}:{value}"
    return None


KEY_FIELDS: Dict[str, List[str]] = {
    "majors": ["majorCode", "slug"],
    "curricula": ["curriculumCode", "slug"],
    "subjects": ["subjectCode", "slug"],
}


def build_supabase_key_map(cursor: psycopg.Cursor, table: str) -> Dict[str, int]:
    fields = KEY_FIELDS.get(table)
    if not fields:
        return {}

    cursor.execute(f"SELECT id, attribute_vn, raw_vn FROM {table}")
    rows = cursor.fetchall()

    key_map: Dict[str, int] = {}
    for row in rows:
        target_id = int(row[0])
        attr = parse_pg_json(row[1])
        raw = parse_pg_json(row[2])
        raw_attr = parse_pg_json(raw.get("attributes"))

        token = find_key_token_from_dicts((attr, raw_attr, raw), fields)
        if token and token not in key_map:
            key_map[token] = target_id

    return key_map


# ---------------------------------------------------------------------------
# Main update logic
# ---------------------------------------------------------------------------

# Tables that have a simple id column (no parent FK needed here)
SIMPLE_TABLES = ["schools", "faculties", "majors", "curricula", "subjects"]


def update_en_columns(sqlite_db_path: str, supabase_db_url: str) -> None:
    if not os.path.exists(sqlite_db_path):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_db_path}")

    pg_url = normalize_postgres_url(supabase_db_url)

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_conn.row_factory = sqlite3.Row

    try:
        with psycopg.connect(pg_url, autocommit=False) as pg_conn:
            with pg_conn.cursor() as cursor:
                for table in SIMPLE_TABLES:
                    rows = sqlite_conn.execute(
                        f"SELECT id, attributes, raw FROM {table} ORDER BY id"
                    ).fetchall()

                    if not rows:
                        print(f"- {table}: no rows in source, skipped")
                        continue

                    cursor.execute(f"SELECT id FROM {table}")
                    target_ids = {int(r[0]) for r in cursor.fetchall()}
                    key_map = build_supabase_key_map(cursor, table)

                    payload_by_target_id: Dict[int, Tuple[Jsonb, Jsonb]] = {}
                    matched_by_offset = 0
                    matched_by_direct_id = 0
                    matched_by_key = 0
                    unmatched = 0

                    for row in rows:
                        src_id = int(row["id"])
                        target_id: Optional[int] = None

                        # EN and VN datasets are usually shifted by +1 id; try that first.
                        if (src_id - 1) in target_ids:
                            target_id = src_id - 1
                            matched_by_offset += 1
                        elif src_id in target_ids:
                            target_id = src_id
                            matched_by_direct_id += 1
                        else:
                            fields = KEY_FIELDS.get(table)
                            if fields:
                                attr = parse_json_text(row["attributes"])
                                raw = parse_json_text(row["raw"])
                                raw_attr = parse_pg_json(raw.get("attributes"))
                                token = find_key_token_from_dicts((attr, raw_attr, raw), fields)
                                if token and token in key_map:
                                    target_id = key_map[token]
                                    matched_by_key += 1

                        if target_id is None:
                            unmatched += 1
                            continue

                        payload_by_target_id[target_id] = (to_jsonb(row["attributes"]), to_jsonb(row["raw"]))

                    if not payload_by_target_id:
                        print(f"- {table}: no matched rows (source={len(rows)}, unmatched={unmatched})")
                        continue

                    payload = [
                        (attribute_en, raw_en, target_id)
                        for target_id, (attribute_en, raw_en) in payload_by_target_id.items()
                    ]

                    cursor.executemany(
                        f"""
                        UPDATE {table}
                        SET attribute_en = %s,
                            raw_en       = %s
                        WHERE id = %s;
                        """,
                        payload,
                    )
                    overwritten_due_duplicate_target = len(rows) - unmatched - len(payload_by_target_id)
                    print(
                        f"- {table}: updated={len(payload_by_target_id)} "
                        f"(source={len(rows)}, offset={matched_by_offset}, direct={matched_by_direct_id}, key={matched_by_key}, "
                        f"unmatched={unmatched}, duplicate_target={max(overwritten_due_duplicate_target, 0)})"
                    )

            pg_conn.commit()

    finally:
        sqlite_conn.close()

    print("\nDone.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="Update attribute_en / raw_en columns on Supabase from syllabus_en.db"
    )
    parser.add_argument(
        "--sqlite-db",
        default="syllabus_en.db",
        help="Path to the English SQLite DB (default: syllabus_en.db)",
    )
    parser.add_argument(
        "--supabase-db-url",
        default=resolve_supabase_url(),
        help=(
            "Supabase PostgreSQL connection string. Supports URL, key-value conninfo, "
            "or values in .env (SUPABASE_DB_URL / SUPABASE_URL or individual vars)."
        ),
    )

    args = parser.parse_args()

    if not args.supabase_db_url:
        print(
            "Missing Supabase DB URL. Pass --supabase-db-url or set SUPABASE_DB_URL "
            "(or SUPABASE_URL / individual SUPABASE_DB_* vars) in .env."
        )
        sys.exit(1)

    try:
        update_en_columns(args.sqlite_db, args.supabase_db_url)
    except Exception as exc:
        print(f"Update failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
