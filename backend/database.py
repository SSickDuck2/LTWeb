import json
from contextlib import contextmanager
from pyexpat import model
from typing import Any, Dict, Generator, List, Optional, Set, Type

from sqlalchemy import Text, cast, delete, func, select, table, text
from sqlalchemy.orm import Session

from backend.orm import (
    Curriculum,
    CurriculumSubject,
    Faculty,
    Major,
    School,
    SessionLocal,
    Subject,
    create_schema,
)

create_schema()

TABLE_MODELS: Dict[str, Type[Any]] = {
    "schools": School,
    "faculties": Faculty,
    "majors": Major,
    "curricula": Curriculum,
    "subjects": Subject,
}

TABLE_FILTERS: Dict[str, Set[str]] = {
    "schools": set(),
    "faculties": {"school_id"},
    "majors": {"faculty_id"},
    "curricula": {"major_id"},
    "subjects": {"curricula_id"},
}


def _validate_table(table: str) -> None:
    if table not in TABLE_MODELS:
        raise ValueError(f"Unsupported table: {table}")


def _parse_json(text: Optional[str]) -> Dict[str, Any]:
    if isinstance(text, dict):
        return text
    if isinstance(text, list):
        return {}
    if not text:
        return {}
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return {}


def check_db_connection() -> Dict[str, Any]:
    with get_db() as db:
        db.execute(text("SELECT 1"))
        schools_count = int(db.scalar(select(func.count()).select_from(School)) or 0)

    return {
        "database": "supabase-postgresql",
        "status": "ok",
        "schoolsCount": schools_count,
    }


def _serialize_item(model_instance: Any) -> Dict[str, Any]:
    return {
        "id": model_instance.id,
        "attributes": _parse_json(model_instance.attribute_vn), 
        "raw": _parse_json(model_instance.raw_vn),
        "attribute_en": _parse_json(model_instance.attribute_en),
        "raw_en": _parse_json(model_instance.raw_en),
    }


def _merge_subject_attributes(base_attributes: Dict[str, Any], link_attributes: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_attributes)

    if "subjectCode" in merged and "subCode" not in merged:
        merged["subCode"] = merged["subjectCode"]

    merged.setdefault("semester", link_attributes.get("semester"))
    merged.setdefault("required", link_attributes.get("required"))
    merged.setdefault("knowledgeBlock", link_attributes.get("knowledgeBlock"))
    merged.setdefault("knowledgeBlockId", link_attributes.get("knowledgeBlockId"))
    merged.setdefault("note", link_attributes.get("note"))

    return merged


@contextmanager
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _build_query(
    table: str,
    id: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    search: Optional[str] = None,
):
    model = TABLE_MODELS[table]
    query = select(model)

    if id is not None:
        query = query.where(model.id == id)

    if filters:
        for key, value in filters.items():
            if value is None:
                continue
            if key not in TABLE_FILTERS[table]:
                raise ValueError(f"Unsupported filter '{key}' for table '{table}'")
            query = query.where(getattr(model, key) == value)

    normalized_search = (search or "").strip()
    if normalized_search:
        query = query.where(cast(model.attribute_vn, Text).ilike(f"%{normalized_search}%"))

    return query


def get_subjects_by_curriculum(
    curricula_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    offset = (page - 1) * page_size
    normalized_search = (search or "").strip()

    with get_db() as db:
        query = (
            select(Subject, CurriculumSubject.link_attributes_vn)
            .join(CurriculumSubject, CurriculumSubject.subject_id == Subject.id)
            .where(CurriculumSubject.curricula_id == curricula_id)
        )

        if normalized_search:
            query = query.where(cast(Subject.attribute_vn, Text).ilike(f"%{normalized_search}%"))

        total = int(db.scalar(select(func.count()).select_from(query.subquery())) or 0)
        rows = db.execute(query.order_by(Subject.id).offset(offset).limit(page_size)).all()

        data = []
        for subject, link_attributes_text in rows:
            base_item = _serialize_item(subject)
            link_attributes = _parse_json(link_attributes_text)
            base_item["attributes"] = _merge_subject_attributes(base_item["attributes"], link_attributes)
            data.append(base_item)

        return {
            "data": data,
            "totalRecords": total,
            "page": page,
            "pageSize": page_size,
            "skippedRecords": offset,
        }


def get_subject_from_curriculum(curricula_id: int, subject_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as db:
        row = db.execute(
            select(Subject, CurriculumSubject.link_attributes_vn)
            .join(CurriculumSubject, CurriculumSubject.subject_id == Subject.id)
            .where(CurriculumSubject.curricula_id == curricula_id)
            .where(Subject.id == subject_id)
        ).first()

        if not row:
            return None

        subject, link_attributes_text = row
        base_item = _serialize_item(subject)
        link_attributes = _parse_json(link_attributes_text)
        base_item["attributes"] = _merge_subject_attributes(base_item["attributes"], link_attributes)
        return base_item


def get_table_data(
    table: str,
    id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
    filters: Optional[Dict[str, Any]] = None,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    _validate_table(table)

    curricula_id = None
    if table == "subjects" and filters:
        curricula_id = filters.get("curricula_id")

    if table == "subjects" and id is None and curricula_id is not None:
        return get_subjects_by_curriculum(
            curricula_id=curricula_id,
            page=page,
            page_size=page_size,
            search=search,
        )

    offset = (page - 1) * page_size

    with get_db() as db:
        query = _build_query(table, id=id, filters=filters, search=search)
        total = int(db.scalar(select(func.count()).select_from(query.subquery())) or 0)

        model = TABLE_MODELS[table]
        if table == "curricula":
            order_clause = model.attribute_vn['name'].as_string().asc()
        else:
            order_clause = model.id.asc()

        items = db.execute(query.order_by(order_clause).offset(offset).limit(page_size)).scalars().all()

        return {
            "data": [_serialize_item(item) for item in items],
            "totalRecords": total,
            "page": page,
            "pageSize": page_size,
            "skippedRecords": offset,
        }


def create_item(
    table: str,
    attributes: Dict[str, Any],
    parent_col: Optional[str] = None,
    parent_id: Optional[int] = None,
) -> int:
    _validate_table(table)

    model = TABLE_MODELS[table]
    attrs_data = attributes
    raw_data = {"attributes": attributes}

    with get_db() as db:
        payload = {
            "attribute_vn": attrs_data,
            "raw_vn": raw_data,
        }

        if table != "subjects" and parent_col and parent_id is not None:
            if parent_col not in TABLE_FILTERS[table]:
                raise ValueError(f"Unsupported parent '{parent_col}' for table '{table}'")
            payload[parent_col] = parent_id

        item = model(**payload)
        db.add(item)
        db.flush()
        created_id = int(item.id)

        if table == "subjects" and parent_col == "curricula_id" and parent_id is not None:
            link = db.get(CurriculumSubject, (parent_id, created_id))
            if link is None:
                db.add(CurriculumSubject(curricula_id=parent_id, subject_id=created_id, link_attributes_vn={}))

        db.commit()
        return created_id


def update_item(table: str, id: int, attributes: Optional[Dict[str, Any]] = None) -> bool:
    _validate_table(table)
    if attributes is None:
        return False

    model = TABLE_MODELS[table]
    attrs_data = attributes
    raw_data = {"attributes": attributes}

    with get_db() as db:
        item = db.get(model, id)
        if item is None:
            return False

        item.attribute_vn = attrs_data
        item.raw_vn = raw_data
        db.commit()
        return True


def delete_item(table: str, id: Optional[int] = None, ids: Optional[List[int]] = None) -> int:
    _validate_table(table)

    target_ids: List[int]
    if id is not None:
        target_ids = [id]
    elif ids:
        target_ids = ids
    else:
        return 0

    model = TABLE_MODELS[table]

    with get_db() as db:
        existing_ids = db.execute(select(model.id).where(model.id.in_(target_ids))).scalars().all()
        if not existing_ids:
            return 0

        if table == "subjects":
            db.execute(delete(CurriculumSubject).where(CurriculumSubject.subject_id.in_(existing_ids)))
        if table == "curricula":
            db.execute(delete(CurriculumSubject).where(CurriculumSubject.curricula_id.in_(existing_ids)))

        result = db.execute(delete(model).where(model.id.in_(existing_ids)))
        db.commit()
        return int(result.rowcount or 0)


def get_single_item(table: str, id: int) -> Optional[Dict[str, Any]]:
    _validate_table(table)
    model = TABLE_MODELS[table]

    with get_db() as db:
        item = db.get(model, id)
        if item is None:
            return None
        return _serialize_item(item)


def migrate_subject_links_from_curricula() -> int:
    created_links = 0

    with get_db() as db:
        rows = db.execute(select(Curriculum.id, Curriculum.attribute_vn)).all()

        for curricula_id, attributes_text in rows:
            curriculum_attributes = _parse_json(attributes_text)
            links = curriculum_attributes.get("curriculum_curriculum_subjects", {}).get("data", [])

            if isinstance(links, dict):
                links = [links]

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

                existing_subject = db.get(Subject, subject_id)
                if existing_subject is None:
                    db.add(Subject(id=subject_id, attribute_vn=subject_attributes, raw_vn=subject))
                else:
                    existing_subject.attribute_vn = subject_attributes
                    existing_subject.raw_vn = subject

                existing_link = db.get(CurriculumSubject, (curricula_id, subject_id))

                if existing_link is None:
                    db.add(
                        CurriculumSubject(
                            curricula_id=curricula_id,
                            subject_id=subject_id,
                            link_attributes_vn=link_attributes,
                        )
                    )
                    created_links += 1
                else:
                    existing_link.link_attributes_vn = link_attributes

        db.commit()

    return created_links


from sqlalchemy import select

def get_scoped_search_suggestions(keyword: str, scope: str, limit_results: int = 2) -> list[dict]:
    """Truy vấn gợi ý tìm kiếm chỉ trong 1 bảng (mục) cụ thể"""
    normalized_search = f"%{keyword.strip()}%"
    suggestions = []

    with get_db() as db:
        if scope == "subjects":
            rows = db.execute(
                select(Subject.id, Subject.attribute_vn['name'].as_string())
                .where(Subject.attribute_vn['name'].as_string().ilike(normalized_search))
                .limit(limit_results)
            ).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/syllabus?subject_id={item_id}"})
                
        elif scope == "majors":
            rows = db.execute(
                select(Major.id, Major.attribute_vn['name'].as_string())
                .where(Major.attribute_vn['name'].as_string().ilike(normalized_search))
                .limit(limit_results)
            ).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/curricula?major_id={item_id}"})

        elif scope == "curricula":
            rows = db.execute(
                select(Curriculum.id, Curriculum.attribute_vn['name'].as_string())
                .where(Curriculum.attribute_vn['name'].as_string().ilike(normalized_search))
                .limit(limit_results)
            ).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/subjects?curricula_id={item_id}"})
                
        elif scope == "faculties":
            rows = db.execute(
                select(Faculty.id, Faculty.attribute_vn['name'].as_string())
                .where(Faculty.attribute_vn['name'].as_string().ilike(normalized_search))
                .limit(limit_results)
            ).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/majors?faculty_id={item_id}"})
                
        elif scope == "schools":
            rows = db.execute(
                select(School.id, School.attribute_vn['name'].as_string())
                .where(School.attribute_vn['name'].as_string().ilike(normalized_search))
                .limit(limit_results)
            ).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/faculties?school_id={item_id}"})

    return suggestions