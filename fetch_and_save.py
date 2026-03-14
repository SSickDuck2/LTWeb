#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Type
from urllib.parse import urlparse, parse_qs

import requests
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from backend.orm import (
    Curriculum,
    CurriculumSubject,
    Faculty,
    Major,
    School,
    Subject,
    create_session_factory,
)


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


TABLE_MODEL_MAP: Dict[str, Type[Any]] = {
    "schools": School,
    "faculties": Faculty,
    "majors": Major,
    "curricula": Curriculum,
    "subjects": Subject,
}


def _json_text(value: Dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False)


def upsert_item(
    db: Session,
    table: str,
    row_id: int,
    attributes: Dict[str, Any],
    raw: Dict[str, Any],
    parent_col: Optional[str] = None,
    parent_id: Optional[int] = None,
) -> None:
    model = TABLE_MODEL_MAP[table]
    payload: Dict[str, Any] = {
        "id": row_id,
        "attributes": _json_text(attributes),
        "raw": _json_text(raw),
    }

    if parent_col and parent_id is not None and table != "subjects":
        payload[parent_col] = parent_id

    update_set: Dict[str, Any] = {
        "attributes": payload["attributes"],
        "raw": payload["raw"],
    }

    if parent_col and parent_id is not None and table != "subjects":
        update_set[parent_col] = parent_id

    stmt = sqlite_insert(model.__table__).values(**payload)
    stmt = stmt.on_conflict_do_update(index_elements=[model.__table__.c.id], set_=update_set)
    db.execute(stmt)


def upsert_curriculum_subject_link(
    db: Session,
    curricula_id: int,
    subject_id: int,
    link_attributes: Dict[str, Any],
) -> None:
    payload = {
        "curricula_id": curricula_id,
        "subject_id": subject_id,
        "link_attributes": _json_text(link_attributes),
    }

    stmt = sqlite_insert(CurriculumSubject.__table__).values(**payload)
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            CurriculumSubject.__table__.c.curricula_id,
            CurriculumSubject.__table__.c.subject_id,
        ],
        set_={"link_attributes": payload["link_attributes"]},
    )
    db.execute(stmt)


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
    return None


def process_and_store(
    db: Session,
    items: List[Dict[str, Any]],
    table: str,
    parent_fragment: Optional[str] = None,
    parent_col: Optional[str] = None,
    default_parent_id: Optional[int] = None,
) -> int:
    count = 0
    for item in items:
        item_id = item.get('id')
        if item_id is None:
            continue

        attributes = item.get('attributes', {}) if isinstance(item, dict) else {}
        parent_id = item.get(parent_col) if parent_col else None
        if parent_id is None and parent_fragment and attributes:
            parent_id = find_parent_id(attributes, parent_fragment)
        if parent_id is None:
            parent_id = default_parent_id

        upsert_item(db, table, int(item_id), attributes, item, parent_col, parent_id)
        count += 1

    return count


def _extract_and_store_curriculum_subjects(db: Session, curriculum_item: Dict[str, Any]) -> int:
    curricula_id = curriculum_item.get("id")
    curriculum_attrs = curriculum_item.get("attributes", {}) if isinstance(curriculum_item, dict) else {}
    if curricula_id is None:
        return 0

    links = curriculum_attrs.get("curriculum_curriculum_subjects", {}).get("data", [])
    if isinstance(links, dict):
        links = [links]

    stored = 0
    for link in links:
        if not isinstance(link, dict):
            continue

        link_attributes = link.get("attributes", {})
        subject = link_attributes.get("curriculum_subject", {}).get("data")
        if not isinstance(subject, dict):
            continue

        subject_id = subject.get("id")
        subject_attributes = subject.get("attributes", {})
        if subject_id is None or not isinstance(subject_attributes, dict):
            continue

        upsert_item(db, "subjects", int(subject_id), subject_attributes, subject)
        upsert_curriculum_subject_link(db, int(curricula_id), int(subject_id), link_attributes)
        stored += 1

    return stored


def _backfill_links_from_curricula_in_db(db: Session) -> int:
    rows = db.execute(select(Curriculum.id, Curriculum.attributes)).all()
    total = 0

    for curricula_id, attrs_text in rows:
        try:
            attrs = json.loads(attrs_text) if attrs_text else {}
        except json.JSONDecodeError:
            continue

        item = {
            "id": curricula_id,
            "attributes": attrs,
        }
        total += _extract_and_store_curriculum_subjects(db, item)

    return total


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
        runtime_engine, SessionFactory = create_session_factory(args.db)
    except Exception as e:
        print('Could not open database:', e)
        print('Hint: Make sure DB Browser for SQLite is CLOSED.')
        sys.exit(1)

    with SessionFactory() as db:
        print('Fetching schools...')
        schools = fetch_all(session, endpoints['schools'])
        n_schools = process_and_store(db, schools, 'schools')
        print(f'Stored {n_schools} schools')

        total_fac = 0
        total_maj = 0
        total_curr = 0
        total_subject_links = 0
        faculty_to_school: Dict[int, int] = {}

        for school in schools:
            attrs = school.get('attributes', {})
            fac_field = next((k for k in attrs.keys() if 'facult' in k.lower()), None)
            if not fac_field:
                continue

            fac_data = attrs.get(fac_field, {}).get('data', [])
            if not fac_data:
                continue

            for fac in fac_data:
                fac['school_id'] = school.get('id')
                if fac.get('id') is not None and school.get('id') is not None:
                    faculty_to_school[int(fac['id'])] = int(school['id'])
            total_fac += process_and_store(db, fac_data, 'faculties', parent_col='school_id')

            for fac in fac_data:
                fac_attrs = fac.get('attributes', {})
                maj_field = next(
                    (k for k in fac_attrs.keys() if 'majors' in k.lower() or 'curriculum_majors' in k.lower()),
                    None,
                )
                if not maj_field:
                    continue

                maj_data = fac_attrs.get(maj_field, {}).get('data', [])
                if not maj_data:
                    continue

                for maj in maj_data:
                    maj['faculty_id'] = fac.get('id')
                total_maj += process_and_store(db, maj_data, 'majors', parent_col='faculty_id')

        print(f'Stored {total_fac} faculties (from schools population)')
        print(f'Stored {total_maj} majors (from faculties population)')

        print('Fetching additional faculties...')
        faculties = fetch_all(session, endpoints['faculties'])

        for fac in faculties:
            fid = fac.get('id')
            if fid is None:
                continue
            mapped_school_id = faculty_to_school.get(int(fid))
            if mapped_school_id is not None:
                fac['school_id'] = mapped_school_id

        additional_fac = process_and_store(db, faculties, 'faculties', parent_col='school_id')
        print(f'Stored/updated {additional_fac} faculties (from faculties endpoint)')

        majors_from_faculties = 0
        for fac in faculties:
            fac_attrs = fac.get('attributes', {})
            maj_field = next(
                (k for k in fac_attrs.keys() if 'majors' in k.lower() or 'curriculum_majors' in k.lower()),
                None,
            )
            if not maj_field:
                continue

            maj_data = fac_attrs.get(maj_field, {}).get('data', [])
            if not maj_data:
                continue

            for maj in maj_data:
                maj['faculty_id'] = fac.get('id')
            majors_from_faculties += process_and_store(db, maj_data, 'majors', parent_col='faculty_id')

        print(f'Stored/updated {majors_from_faculties} majors (from faculties endpoint)')

        print('Fetching majors for curricula/subjects...')
        majors_detailed = fetch_all(session, endpoints['majors'])
        print(f'Retrieved {len(majors_detailed)} detailed majors')
        if majors_detailed:
            print('First major keys:', list(majors_detailed[0].get('attributes', {}).keys()))

        existing_major_faculty = {
            major_id: faculty_id
            for major_id, faculty_id in db.execute(select(Major.id, Major.faculty_id)).all()
            if faculty_id is not None
        }

        for maj in majors_detailed:
            if maj.get('faculty_id') is None:
                mapped_faculty = existing_major_faculty.get(maj.get('id'))
                if mapped_faculty is not None:
                    maj['faculty_id'] = mapped_faculty

        detailed_major_count = process_and_store(db, majors_detailed, 'majors', parent_col='faculty_id')
        print(f'Stored/updated {detailed_major_count} majors (from detailed majors endpoint)')

        for major in majors_detailed:
            major_attrs = major.get('attributes', {})
            curricula_field = next(
                (k for k in major_attrs.keys() if 'curricula' in k.lower() or 'curriculum' in k.lower()),
                None,
            )
            if not curricula_field:
                continue

            curricula_data = major_attrs.get(curricula_field, {}).get('data', [])
            if isinstance(curricula_data, dict):
                curricula_data = [curricula_data]
            if not curricula_data:
                continue

            for curriculum in curricula_data:
                curriculum['major_id'] = major.get('id')

            total_curr += process_and_store(db, curricula_data, 'curricula', parent_col='major_id')

            for curriculum in curricula_data:
                total_subject_links += _extract_and_store_curriculum_subjects(db, curriculum)

        print(f'Stored/updated {total_curr} curricula (from majors endpoint)')
        print(f'Stored/updated {total_subject_links} curriculum-subject links (from majors endpoint)')

        print('Backfilling links from all curricula already in DB...')
        backfilled_links = _backfill_links_from_curricula_in_db(db)
        print(f'Backfilled/updated {backfilled_links} curriculum-subject links (from stored curricula JSON)')

        db.commit()

    runtime_engine.dispose()


if __name__ == '__main__':
    main()
