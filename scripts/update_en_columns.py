#!/usr/bin/env python3
"""
Import CSV data into Supabase *_new tables.

Supported mappings:
- faculties_rows.csv -> faculties_new
- majors_rows.csv -> majors_new
- curriculum_rows.csv -> curriculum_new
- curriculum_subjects_rows.csv -> subjects_new
- curriculum_subjects_rows.csv -> curriculum_subjects_new

Usage:
    python scripts/update_en_columns.py --targets all
    python scripts/update_en_columns.py --targets faculties majors subjects curriculum_subjects
"""

import argparse
import csv
import json
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Iterable, Optional, Sequence

import psycopg
from psycopg.types.json import Jsonb


# ---------------------------------------------------------------------------
# Env / connection helpers
# ---------------------------------------------------------------------------

def load_env_file(env_path: str = ".env") -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as file_obj:
        for raw_line in file_obj:
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
            value = _first_non_empty_env(env_keys)
            if value:
                parts[field] = value

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
# Parsing helpers
# ---------------------------------------------------------------------------

def set_csv_field_size_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def parse_json_text(value: Optional[str]) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except json.JSONDecodeError:
        return {}


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def to_int(value: Any) -> Optional[int]:
    text = normalize_text(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def to_decimal(value: Any) -> Optional[Decimal]:
    text = normalize_text(value)
    if text is None:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = normalize_text(value)
    if text is None:
        return None
    low = text.lower()
    if low in {"1", "true", "yes", "y"}:
        return True
    if low in {"0", "false", "no", "n"}:
        return False
    return None


def parse_semester(value: Any) -> Optional[int]:
    text = normalize_text(value)
    if text is None:
        return None
    num = re.search(r"(\d+)", text)
    if not num:
        return None
    return to_int(num.group(1))


def pick(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def extract_code(attrs: Dict[str, Any], *candidate_keys: str) -> Optional[str]:
    for key in candidate_keys:
        value = normalize_text(attrs.get(key))
        if value:
            return value
    return None


def to_jsonb_obj(data: Dict[str, Any]) -> Jsonb:
    return Jsonb(data if isinstance(data, dict) else {})


# ---------------------------------------------------------------------------
# Row mapping for each table
# ---------------------------------------------------------------------------

def map_curriculum_row(csv_row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    row_id = to_int(csv_row.get("id"))
    if row_id is None:
        return None

    vn = parse_json_text(csv_row.get("attribute_vn"))
    en = parse_json_text(csv_row.get("attribute_en"))
    raw_vn = parse_json_text(csv_row.get("raw_vn"))
    raw_en = parse_json_text(csv_row.get("raw_en"))

    year = to_int(vn.get("effective_year"))
    if year is None:
        year = to_int(vn.get("effectiveYear"))
    if year is None:
        year_text = normalize_text(vn.get("year"))
        if year_text:
            matches = re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", year_text)
            if matches:
                year = int(matches[-1])

    return {
        "id": row_id,
        "major_id": to_int(csv_row.get("major_id")),
        "name": normalize_text(vn.get("name")),
        "slug": normalize_text(vn.get("slug")),
        "locale": normalize_text(vn.get("locale")),
        "description": normalize_text(vn.get("description")),
        "code": extract_code(vn, "code", "curriculumCode", "curriculaCode"),
        "credits": to_decimal(vn.get("credits")),
        "effective_year": year,
        "created_at": normalize_text(pick(vn.get("createdAt"), en.get("createdAt"))),
        "updated_at": normalize_text(pick(vn.get("updatedAt"), en.get("updatedAt"))),
        "published_at": normalize_text(pick(vn.get("publishedAt"), en.get("publishedAt"))),
        "raw_attributes": to_jsonb_obj(raw_vn),
        "vn_name": normalize_text(vn.get("name")),
        "vn_slug": normalize_text(vn.get("slug")),
        "vn_locale": normalize_text(vn.get("locale")),
        "vn_description": normalize_text(vn.get("description")),
        "vn_code": extract_code(vn, "code", "curriculumCode", "curriculaCode"),
        "vn_raw_attributes": to_jsonb_obj(raw_vn),
        "en_name": normalize_text(en.get("name")),
        "en_slug": normalize_text(en.get("slug")),
        "en_locale": normalize_text(en.get("locale")),
        "en_description": normalize_text(en.get("description")),
        "en_code": extract_code(en, "code", "curriculumCode", "curriculaCode"),
        "en_raw_attributes": to_jsonb_obj(raw_en),
    }


def map_faculty_row(csv_row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    row_id = to_int(csv_row.get("id"))
    if row_id is None:
        return None

    vn = parse_json_text(csv_row.get("attribute_vn"))
    en = parse_json_text(csv_row.get("attribute_en"))
    raw_vn = parse_json_text(csv_row.get("raw_vn"))
    raw_en = parse_json_text(csv_row.get("raw_en"))

    return {
        "id": row_id,
        "school_id": to_int(csv_row.get("school_id")),
        "en_name": normalize_text(en.get("name")),
        "en_slug": normalize_text(en.get("slug")),
        "en_locale": normalize_text(en.get("locale")),
        "en_description": normalize_text(en.get("description")),
        "en_code": extract_code(en, "code", "facultyCode"),
        "created_at": normalize_text(pick(vn.get("createdAt"), en.get("createdAt"))),
        "updated_at": normalize_text(pick(vn.get("updatedAt"), en.get("updatedAt"))),
        "published_at": normalize_text(pick(vn.get("publishedAt"), en.get("publishedAt"))),
        "en_raw_attributes": to_jsonb_obj(raw_en),
        "vn_name": normalize_text(vn.get("name")),
        "vn_slug": normalize_text(vn.get("slug")),
        "vn_locale": normalize_text(vn.get("locale")),
        "vn_description": normalize_text(vn.get("description")),
        "vn_code": extract_code(vn, "code", "facultyCode"),
        "vn_raw_attributes": to_jsonb_obj(raw_vn),
    }


def map_major_row(csv_row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    row_id = to_int(csv_row.get("id"))
    if row_id is None:
        return None

    vn = parse_json_text(csv_row.get("attribute_vn"))
    en = parse_json_text(csv_row.get("attribute_en"))
    raw_vn = parse_json_text(csv_row.get("raw_vn"))
    raw_en = parse_json_text(csv_row.get("raw_en"))

    code_vn = extract_code(vn, "majorCode", "code", "facultyCode")
    code_en = extract_code(en, "majorCode", "code", "facultyCode")

    return {
        "id": row_id,
        "faculty_id": to_int(csv_row.get("faculty_id")),
        "name": normalize_text(vn.get("name")),
        "slug": normalize_text(vn.get("slug")),
        "locale": normalize_text(vn.get("locale")),
        "description": normalize_text(vn.get("description")),
        "faculty_code": code_vn,
        "major_code": code_vn,
        "created_at": normalize_text(pick(vn.get("createdAt"), en.get("createdAt"))),
        "updated_at": normalize_text(pick(vn.get("updatedAt"), en.get("updatedAt"))),
        "published_at": normalize_text(pick(vn.get("publishedAt"), en.get("publishedAt"))),
        "raw_attributes": to_jsonb_obj(raw_vn),
        "vn_name": normalize_text(vn.get("name")),
        "vn_slug": normalize_text(vn.get("slug")),
        "vn_locale": normalize_text(vn.get("locale")),
        "vn_description": normalize_text(vn.get("description")),
        "vn_faculty_code": code_vn,
        "vn_major_code": code_vn,
        "vn_raw_attributes": to_jsonb_obj(raw_vn),
        "en_name": normalize_text(en.get("name")),
        "en_slug": normalize_text(en.get("slug")),
        "en_locale": normalize_text(en.get("locale")),
        "en_description": normalize_text(en.get("description")),
        "en_faculty_code": code_en,
        "en_major_code": code_en,
        "en_raw_attributes": to_jsonb_obj(raw_en),
    }


def map_curriculum_subject_row(csv_row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    curriculum_id = to_int(csv_row.get("curricula_id"))
    subject_id = to_int(csv_row.get("subject_id"))
    if curriculum_id is None or subject_id is None:
        return None

    vn = parse_json_text(csv_row.get("link_attributes_vn"))
    en = parse_json_text(csv_row.get("link_attributes_en"))

    vn_subject_attrs = (
        vn.get("curriculum_subject", {})
        .get("data", {})
        .get("attributes", {})
    )
    en_subject_attrs = (
        en.get("curriculum_subject", {})
        .get("data", {})
        .get("attributes", {})
    )

    return {
        "id": curriculum_id * 10_000_000 + subject_id,
        "curriculum_id": curriculum_id,
        "curricula_id": curriculum_id,
        "subject_id": subject_id,
        "semester": parse_semester(pick(vn.get("semester"), en.get("semester"))),
        "year": to_int(pick(vn.get("year"), en.get("year"))),
        "mandatory": to_bool(pick(vn.get("required"), en.get("required"), vn.get("mandatory"), en.get("mandatory"))),
        "credit_value": to_decimal(
            pick(
                vn.get("credits"),
                en.get("credits"),
                vn_subject_attrs.get("credits"),
                en_subject_attrs.get("credits"),
            )
        ),
        "link_note": normalize_text(pick(vn.get("note"), en.get("note"))),
        "link_attributes": to_jsonb_obj(vn if vn else en),
        "created_at": normalize_text(pick(vn.get("createdAt"), en.get("createdAt"))),
        "updated_at": normalize_text(pick(vn.get("updatedAt"), en.get("updatedAt"))),
        "vn_note": normalize_text(pick(vn.get("note"), en.get("note"))),
        "vn_language": normalize_text(pick(vn.get("language"), en.get("language"))),
        "vn_curriculum_subject_name": normalize_text(pick(vn_subject_attrs.get("name"), en_subject_attrs.get("name"))),
        "vn_curriculum_subject_slug": normalize_text(pick(vn_subject_attrs.get("slug"), en_subject_attrs.get("slug"))),
        "en_note": normalize_text(pick(en.get("note"), vn.get("note"))),
        "en_language": normalize_text(pick(en.get("language"), vn.get("language"))),
        "en_curriculum_subject_name": normalize_text(pick(en_subject_attrs.get("name"), vn_subject_attrs.get("name"))),
        "en_curriculum_subject_slug": normalize_text(pick(en_subject_attrs.get("slug"), vn_subject_attrs.get("slug"))),
    }


def map_subject_row_from_link(csv_row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    subject_id = to_int(csv_row.get("subject_id"))
    if subject_id is None:
        return None

    vn = parse_json_text(csv_row.get("link_attributes_vn"))
    en = parse_json_text(csv_row.get("link_attributes_en"))

    vn_subject_data = vn.get("curriculum_subject", {}).get("data", {})
    en_subject_data = en.get("curriculum_subject", {}).get("data", {})

    vn_attr = vn_subject_data.get("attributes", {}) if isinstance(vn_subject_data, dict) else {}
    en_attr = en_subject_data.get("attributes", {}) if isinstance(en_subject_data, dict) else {}

    return {
        "id": subject_id,
        "code": extract_code(vn_attr, "subjectCode", "code"),
        "slug": normalize_text(vn_attr.get("slug")),
        "name": normalize_text(vn_attr.get("name")),
        "locale": normalize_text(vn_attr.get("locale")),
        "short_name": normalize_text(pick(vn_attr.get("shortName"), vn_attr.get("short_name"))),
        "description": normalize_text(vn_attr.get("description")),
        "credits": to_decimal(vn_attr.get("credits")),
        "lecture_hours": to_int(pick(vn_attr.get("theoryLessons"), vn_attr.get("lectureHours"), vn_attr.get("lecture_hours"))),
        "practice_hours": to_int(pick(vn_attr.get("practiceLessons"), vn_attr.get("practiceHours"), vn_attr.get("practice_hours"))),
        "created_at": normalize_text(pick(vn_attr.get("createdAt"), en_attr.get("createdAt"))),
        "updated_at": normalize_text(pick(vn_attr.get("updatedAt"), en_attr.get("updatedAt"))),
        "published_at": normalize_text(pick(vn_attr.get("publishedAt"), en_attr.get("publishedAt"))),
        "raw_attributes": to_jsonb_obj(vn_subject_data if isinstance(vn_subject_data, dict) else vn_attr),
        "en_name": normalize_text(pick(en_attr.get("name"), vn_attr.get("name"))),
        "en_slug": normalize_text(pick(en_attr.get("slug"), vn_attr.get("slug"))),
        "en_locale": normalize_text(pick(en_attr.get("locale"), vn_attr.get("locale"))),
        "en_short_name": normalize_text(pick(en_attr.get("shortName"), en_attr.get("short_name"), vn_attr.get("shortName"), vn_attr.get("short_name"))),
        "en_description": normalize_text(pick(en_attr.get("description"), vn_attr.get("description"))),
        "en_code": extract_code(en_attr, "subjectCode", "code") or extract_code(vn_attr, "subjectCode", "code"),
        "en_raw_attributes": to_jsonb_obj(en_subject_data if isinstance(en_subject_data, dict) else en_attr),
        "vn_name": normalize_text(pick(vn_attr.get("name"), en_attr.get("name"))),
        "vn_slug": normalize_text(pick(vn_attr.get("slug"), en_attr.get("slug"))),
        "vn_locale": normalize_text(pick(vn_attr.get("locale"), en_attr.get("locale"))),
        "vn_short_name": normalize_text(pick(vn_attr.get("shortName"), vn_attr.get("short_name"), en_attr.get("shortName"), en_attr.get("short_name"))),
        "vn_description": normalize_text(pick(vn_attr.get("description"), en_attr.get("description"))),
        "vn_code": extract_code(vn_attr, "subjectCode", "code") or extract_code(en_attr, "subjectCode", "code"),
        "vn_raw_attributes": to_jsonb_obj(vn_subject_data if isinstance(vn_subject_data, dict) else vn_attr),
    }


def _extract_subject_map_from_curriculum_attrs(attrs: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    result: Dict[int, Dict[str, Any]] = {}

    links = attrs.get("curriculum_curriculum_subjects", {}).get("data", [])
    if isinstance(links, dict):
        links = [links]
    if not isinstance(links, list):
        return result

    for link in links:
        if not isinstance(link, dict):
            continue

        link_attrs = link.get("attributes", {})
        if not isinstance(link_attrs, dict):
            continue

        subject_data = link_attrs.get("curriculum_subject", {}).get("data")
        if not isinstance(subject_data, dict):
            continue

        subject_id = to_int(subject_data.get("id"))
        if subject_id is None:
            continue

        subject_attr = subject_data.get("attributes", {})
        if not isinstance(subject_attr, dict):
            subject_attr = {}

        result[subject_id] = {
            "data": subject_data,
            "attributes": subject_attr,
        }

    return result


def map_subject_rows_from_curriculum(csv_row: Dict[str, str]) -> list[Dict[str, Any]]:
    vn = parse_json_text(csv_row.get("attribute_vn"))
    en = parse_json_text(csv_row.get("attribute_en"))

    vn_subject_map = _extract_subject_map_from_curriculum_attrs(vn)
    en_subject_map = _extract_subject_map_from_curriculum_attrs(en)

    all_subject_ids = set(vn_subject_map.keys()) | set(en_subject_map.keys())
    if not all_subject_ids:
        return []

    rows: list[Dict[str, Any]] = []
    for subject_id in all_subject_ids:
        vn_subject = vn_subject_map.get(subject_id, {})
        en_subject = en_subject_map.get(subject_id, {})

        vn_attr = vn_subject.get("attributes", {}) if isinstance(vn_subject, dict) else {}
        en_attr = en_subject.get("attributes", {}) if isinstance(en_subject, dict) else {}

        vn_data = vn_subject.get("data", {}) if isinstance(vn_subject, dict) else {}
        en_data = en_subject.get("data", {}) if isinstance(en_subject, dict) else {}

        if not isinstance(vn_attr, dict):
            vn_attr = {}
        if not isinstance(en_attr, dict):
            en_attr = {}

        rows.append(
            {
                "id": subject_id,
                "code": extract_code(vn_attr, "subjectCode", "code") or extract_code(en_attr, "subjectCode", "code"),
                "slug": normalize_text(vn_attr.get("slug")) or normalize_text(en_attr.get("slug")),
                "name": normalize_text(vn_attr.get("name")) or normalize_text(en_attr.get("name")),
                "locale": normalize_text(vn_attr.get("locale")) or normalize_text(en_attr.get("locale")),
                "short_name": normalize_text(pick(vn_attr.get("shortName"), vn_attr.get("short_name"), en_attr.get("shortName"), en_attr.get("short_name"))),
                "description": normalize_text(vn_attr.get("description")) or normalize_text(en_attr.get("description")),
                "credits": to_decimal(vn_attr.get("credits")) or to_decimal(en_attr.get("credits")),
                "lecture_hours": to_int(pick(vn_attr.get("theoryLessons"), vn_attr.get("lectureHours"), vn_attr.get("lecture_hours"), en_attr.get("theoryLessons"), en_attr.get("lectureHours"), en_attr.get("lecture_hours"))),
                "practice_hours": to_int(pick(vn_attr.get("practiceLessons"), vn_attr.get("practiceHours"), vn_attr.get("practice_hours"), en_attr.get("practiceLessons"), en_attr.get("practiceHours"), en_attr.get("practice_hours"))),
                "created_at": normalize_text(pick(vn_attr.get("createdAt"), en_attr.get("createdAt"))),
                "updated_at": normalize_text(pick(vn_attr.get("updatedAt"), en_attr.get("updatedAt"))),
                "published_at": normalize_text(pick(vn_attr.get("publishedAt"), en_attr.get("publishedAt"))),
                "raw_attributes": to_jsonb_obj(vn_data if isinstance(vn_data, dict) and vn_data else en_data if isinstance(en_data, dict) else {}),
                "en_name": normalize_text(en_attr.get("name")),
                "en_slug": normalize_text(en_attr.get("slug")),
                "en_locale": normalize_text(en_attr.get("locale")),
                "en_short_name": normalize_text(pick(en_attr.get("shortName"), en_attr.get("short_name"))),
                "en_description": normalize_text(en_attr.get("description")),
                "en_code": extract_code(en_attr, "subjectCode", "code"),
                "en_raw_attributes": to_jsonb_obj(en_data if isinstance(en_data, dict) else {}),
                "vn_name": normalize_text(vn_attr.get("name")),
                "vn_slug": normalize_text(vn_attr.get("slug")),
                "vn_locale": normalize_text(vn_attr.get("locale")),
                "vn_short_name": normalize_text(pick(vn_attr.get("shortName"), vn_attr.get("short_name"))),
                "vn_description": normalize_text(vn_attr.get("description")),
                "vn_code": extract_code(vn_attr, "subjectCode", "code"),
                "vn_raw_attributes": to_jsonb_obj(vn_data if isinstance(vn_data, dict) else {}),
            }
        )

    return rows


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_table_columns(cursor: psycopg.Cursor, table_name: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cursor.fetchall()}


def build_upsert_sql(table_name: str, columns: list[str], conflict_columns: list[str]) -> str:
    q_table = quote_ident(table_name)
    q_cols = [quote_ident(c) for c in columns]
    placeholders = ["%s"] * len(columns)

    valid_conflicts = [c for c in conflict_columns if c in columns]
    updates = [c for c in columns if c not in valid_conflicts]

    insert_part = (
        f"INSERT INTO {q_table} ({', '.join(q_cols)}) "
        f"VALUES ({', '.join(placeholders)})"
    )

    if not valid_conflicts:
        return insert_part

    if not updates:
        return f"{insert_part} ON CONFLICT ({', '.join(quote_ident(c) for c in valid_conflicts)}) DO NOTHING"

    update_part = ", ".join(f"{quote_ident(c)} = EXCLUDED.{quote_ident(c)}" for c in updates)
    return (
        f"{insert_part} "
        f"ON CONFLICT ({', '.join(quote_ident(c) for c in valid_conflicts)}) "
        f"DO UPDATE SET {update_part}"
    )


def execute_batch_upsert(
    cursor: psycopg.Cursor,
    table_name: str,
    rows: list[Dict[str, Any]],
    conflict_preferences: list[list[str]],
) -> int:
    if not rows:
        return 0

    table_columns = get_table_columns(cursor, table_name)
    if not table_columns:
        raise RuntimeError(f"Target table not found or has no columns: {table_name}")

    filtered_rows = []
    for row in rows:
        filtered = {k: v for k, v in row.items() if k in table_columns}
        if filtered:
            filtered_rows.append(filtered)

    if not filtered_rows:
        return 0

    ordered_columns: list[str] = []
    seen: set[str] = set()
    for row in filtered_rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                ordered_columns.append(key)

    conflict_columns: list[str] = []
    for candidate in conflict_preferences:
        if all(col in table_columns for col in candidate):
            conflict_columns = candidate
            break

    sql = build_upsert_sql(table_name, ordered_columns, conflict_columns)
    params = [tuple(row.get(col) for col in ordered_columns) for row in filtered_rows]
    cursor.executemany(sql, params)
    return len(filtered_rows)


# ---------------------------------------------------------------------------
# Import jobs
# ---------------------------------------------------------------------------

MapFn = Callable[[Dict[str, str]], Optional[Dict[str, Any] | list[Dict[str, Any]]]]

JOBS: Dict[str, Dict[str, Any]] = {
    "faculties": {
        "csv": "faculties_rows.csv",
        "table": "faculties_new",
        "map_fn": map_faculty_row,
        "conflicts": [["id"]],
    },
    "majors": {
        "csv": "majors_rows.csv",
        "table": "majors_new",
        "map_fn": map_major_row,
        "conflicts": [["id"]],
    },
    "curriculum": {
        "csv": "curriculum_rows.csv",
        "table": "curriculum_new",
        "map_fn": map_curriculum_row,
        "conflicts": [["id"]],
    },
    "subjects": {
        "csv": "curriculum_rows.csv",
        "table": "subjects_new",
        "map_fn": map_subject_rows_from_curriculum,
        "conflicts": [["id"]],
    },
    "curriculum_subjects": {
        "csv": "curriculum_subjects_rows.csv",
        "table": "curriculum_subjects_new",
        "map_fn": map_curriculum_subject_row,
        "conflicts": [["id"], ["curricula_id", "subject_id"]],
    },
}


def import_one_job(
    cursor: psycopg.Cursor,
    csv_path: str,
    table_name: str,
    map_fn: MapFn,
    conflict_preferences: list[list[str]],
    batch_size: int,
) -> tuple[int, int, int]:
    resolved_csv_path = csv_path
    if not os.path.exists(resolved_csv_path):
        candidate = os.path.join("data", csv_path)
        if os.path.exists(candidate):
            resolved_csv_path = candidate
        else:
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

    source_rows = 0
    mapped_rows = 0
    upserted_rows = 0

    with open(resolved_csv_path, "r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        batch: list[Dict[str, Any]] = []

        for raw_row in reader:
            source_rows += 1
            mapped = map_fn(raw_row)
            if mapped is None:
                continue

            if isinstance(mapped, list):
                if not mapped:
                    continue
                mapped_rows += len(mapped)
                batch.extend(mapped)
            else:
                mapped_rows += 1
                batch.append(mapped)

            if len(batch) >= batch_size:
                upserted_rows += execute_batch_upsert(cursor, table_name, batch, conflict_preferences)
                batch.clear()

        if batch:
            upserted_rows += execute_batch_upsert(cursor, table_name, batch, conflict_preferences)

    return source_rows, mapped_rows, upserted_rows


def import_jobs(targets: list[str], supabase_db_url: str, batch_size: int) -> None:
    pg_url = normalize_postgres_url(supabase_db_url)

    with psycopg.connect(pg_url, autocommit=False) as pg_conn:
        with pg_conn.cursor() as cursor:
            for key in targets:
                job = JOBS[key]
                source_rows, mapped_rows, upserted_rows = import_one_job(
                    cursor=cursor,
                    csv_path=job["csv"],
                    table_name=job["table"],
                    map_fn=job["map_fn"],
                    conflict_preferences=job["conflicts"],
                    batch_size=batch_size,
                )
                print(
                    f"- {key}: table={job['table']}, csv={job['csv']}, "
                    f"source={source_rows}, mapped={mapped_rows}, upserted={upserted_rows}"
                )

        pg_conn.commit()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    load_env_file()
    set_csv_field_size_limit()

    parser = argparse.ArgumentParser(
        description="Import CSV data into Supabase *_new tables"
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=["all", *JOBS.keys()],
        default=["all"],
        help="Import targets (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per batch upsert (default: 1000)",
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

    if args.batch_size <= 0:
        print("--batch-size must be a positive integer")
        sys.exit(1)

    if not args.supabase_db_url:
        print(
            "Missing Supabase DB URL. Pass --supabase-db-url or set SUPABASE_DB_URL "
            "(or SUPABASE_URL / individual SUPABASE_DB_* vars) in .env."
        )
        sys.exit(1)

    selected_targets = list(JOBS.keys()) if "all" in args.targets else args.targets

    try:
        print("Starting import...")
        import_jobs(selected_targets, args.supabase_db_url, args.batch_size)
        print("Import completed.")
    except Exception as exc:
        print(f"Import failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
