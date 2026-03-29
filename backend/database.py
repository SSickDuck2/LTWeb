import json
from typing import Any, Dict, Generator, List, Optional, Set, Type

from sqlalchemy import Text, cast, delete, func, or_, select, text
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


def _parse_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {}
    if not value:
        return {}
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def check_db_connection() -> Dict[str, Any]:
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        schools_count = int(db.scalar(select(func.count()).select_from(School)) or 0)
    finally:
        db.close()

    return {
        "database": "supabase-postgresql",
        "status": "ok",
        "schoolsCount": schools_count,
    }


def _serialize_school(item: School) -> Dict[str, Any]:
    vn_name = _coalesce(item.vn_name, item.en_name)
    vn_slug = _coalesce(item.vn_slug, item.en_slug)
    vn_locale = _coalesce(item.vn_locale, item.en_locale)
    vn_description = _coalesce(item.vn_description, item.en_description)
    vn_code = _coalesce(item.vn_code, item.en_code)

    en_name = _coalesce(item.en_name, item.vn_name)
    en_slug = _coalesce(item.en_slug, item.vn_slug)
    en_locale = _coalesce(item.en_locale, item.vn_locale)
    en_description = _coalesce(item.en_description, item.vn_description)
    en_code = _coalesce(item.en_code, item.vn_code)

    vn = {
        "name": vn_name,
        "slug": vn_slug,
        "locale": vn_locale,
        "description": vn_description,
        "schoolCode": vn_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    en = {
        "name": en_name,
        "slug": en_slug,
        "locale": en_locale,
        "description": en_description,
        "schoolCode": en_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    return {
        "id": item.id,
        "attributes": vn,
        "raw": _parse_json(_coalesce(item.vn_raw_attributes, item.en_raw_attributes)),
        "attribute_en": en,
        "raw_en": _parse_json(item.en_raw_attributes),
    }


def _serialize_faculty(item: Faculty) -> Dict[str, Any]:
    vn = {
        "name": item.vn_name,
        "slug": item.vn_slug,
        "locale": item.vn_locale,
        "description": item.vn_description,
        "facultyCode": item.vn_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    en = {
        "name": item.en_name,
        "slug": item.en_slug,
        "locale": item.en_locale,
        "description": item.en_description,
        "facultyCode": item.en_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    return {
        "id": item.id,
        "attributes": vn,
        "raw": _parse_json(item.vn_raw_attributes),
        "attribute_en": en,
        "raw_en": _parse_json(item.en_raw_attributes),
    }


def _serialize_major(item: Major) -> Dict[str, Any]:
    vn_name = _coalesce(item.vn_name, item.name)
    vn_slug = _coalesce(item.vn_slug, item.slug)
    vn_locale = _coalesce(item.vn_locale, item.locale)
    vn_description = _coalesce(item.vn_description, item.description)
    vn_code = _coalesce(item.vn_faculty_code, item.faculty_code)

    en_name = _coalesce(item.en_name, item.name)
    en_slug = _coalesce(item.en_slug, item.slug)
    en_locale = _coalesce(item.en_locale, item.locale)
    en_description = _coalesce(item.en_description, item.description)
    en_code = _coalesce(item.en_faculty_code, item.faculty_code)

    vn = {
        "name": vn_name,
        "slug": vn_slug,
        "locale": vn_locale,
        "description": vn_description,
        "majorCode": vn_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    en = {
        "name": en_name,
        "slug": en_slug,
        "locale": en_locale,
        "description": en_description,
        "majorCode": en_code,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    return {
        "id": item.id,
        "attributes": vn,
        "raw": _parse_json(_coalesce(item.vn_raw_attributes, item.raw_attributes)),
        "attribute_en": en,
        "raw_en": _parse_json(item.en_raw_attributes),
    }


def _serialize_curriculum(item: Curriculum) -> Dict[str, Any]:
    vn_name = _coalesce(item.vn_name, item.name)
    vn_slug = _coalesce(item.vn_slug, item.slug)
    vn_locale = _coalesce(item.vn_locale, item.locale)
    vn_description = _coalesce(item.vn_description, item.description)
    vn_code = _coalesce(item.vn_code, item.code)

    en_name = _coalesce(item.en_name, item.name)
    en_slug = _coalesce(item.en_slug, item.slug)
    en_locale = _coalesce(item.en_locale, item.locale)
    en_description = _coalesce(item.en_description, item.description)
    en_code = _coalesce(item.en_code, item.code)

    vn = {
        "name": vn_name,
        "slug": vn_slug,
        "locale": vn_locale,
        "description": vn_description,
        "curriculumCode": vn_code,
        "credits": float(item.credits) if item.credits is not None else None,
        "effectiveYear": item.effective_year,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    en = {
        "name": en_name,
        "slug": en_slug,
        "locale": en_locale,
        "description": en_description,
        "curriculumCode": en_code,
        "credits": float(item.credits) if item.credits is not None else None,
        "effectiveYear": item.effective_year,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    return {
        "id": item.id,
        "attributes": vn,
        "raw": _parse_json(_coalesce(item.vn_raw_attributes, item.raw_attributes)),
        "attribute_en": en,
        "raw_en": _parse_json(item.en_raw_attributes),
    }


def _serialize_subject(item: Subject) -> Dict[str, Any]:
    vn_name = _coalesce(item.vn_name, item.name if item.locale == "vi" else None)
    vn_slug = _coalesce(item.vn_slug, item.slug if item.locale == "vi" else None)
    vn_locale = _coalesce(item.vn_locale, item.locale if item.locale == "vi" else None)
    vn_description = _coalesce(item.vn_description, item.description if item.locale == "vi" else None)
    vn_short_name = _coalesce(item.vn_short_name, item.short_name if item.locale == "vi" else None)
    vn_code = _coalesce(item.vn_code, item.code if item.locale == "vi" else None, item.code)

    en_name = _coalesce(item.en_name, item.name if item.locale == "en" else None)
    en_slug = _coalesce(item.en_slug, item.slug if item.locale == "en" else None)
    en_locale = _coalesce(item.en_locale, item.locale if item.locale == "en" else None)
    en_description = _coalesce(item.en_description, item.description if item.locale == "en" else None)
    en_short_name = _coalesce(item.en_short_name, item.short_name if item.locale == "en" else None)
    en_code = _coalesce(item.en_code, item.code if item.locale == "en" else None, item.code)

    vn = {
        "name": vn_name,
        "slug": vn_slug,
        "locale": vn_locale,
        "shortName": vn_short_name,
        "description": vn_description,
        "subjectCode": vn_code,
        "subCode": vn_code,
        "credits": float(item.credits) if item.credits is not None else None,
        "theoryLessons": item.lecture_hours,
        "practiceLessons": item.practice_hours,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    en = {
        "name": en_name,
        "slug": en_slug,
        "locale": en_locale,
        "shortName": en_short_name,
        "description": en_description,
        "subjectCode": en_code,
        "subCode": en_code,
        "credits": float(item.credits) if item.credits is not None else None,
        "theoryLessons": item.lecture_hours,
        "practiceLessons": item.practice_hours,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "updatedAt": item.updated_at.isoformat() if item.updated_at else None,
        "publishedAt": item.published_at.isoformat() if item.published_at else None,
    }
    return {
        "id": item.id,
        "attributes": vn,
        "raw": _parse_json(_coalesce(item.vn_raw_attributes, item.raw_attributes)),
        "attribute_en": en,
        "raw_en": _parse_json(item.en_raw_attributes),
    }


def _serialize_item(model_instance: Any) -> Dict[str, Any]:
    if isinstance(model_instance, School):
        return _serialize_school(model_instance)
    if isinstance(model_instance, Faculty):
        return _serialize_faculty(model_instance)
    if isinstance(model_instance, Major):
        return _serialize_major(model_instance)
    if isinstance(model_instance, Curriculum):
        return _serialize_curriculum(model_instance)
    if isinstance(model_instance, Subject):
        return _serialize_subject(model_instance)
    raise ValueError("Unsupported model instance")


def _merge_subject_attributes(base_attributes: Dict[str, Any], link: CurriculumSubject, lang: str = "vi") -> Dict[str, Any]:
    merged = dict(base_attributes)
    merged.setdefault("semester", link.semester)
    merged.setdefault("required", link.mandatory)
    merged.setdefault("note", link.vn_note if lang == "vi" else link.en_note)
    merged.setdefault("language", link.vn_language if lang == "vi" else link.en_language)

    if lang == "vi":
        merged.setdefault("name", _coalesce(link.vn_curriculum_subject_name, merged.get("name")))
        merged.setdefault("slug", _coalesce(link.vn_curriculum_subject_slug, merged.get("slug")))
    else:
        merged.setdefault("name", _coalesce(link.en_curriculum_subject_name, merged.get("name")))
        merged.setdefault("slug", _coalesce(link.en_curriculum_subject_slug, merged.get("slug")))

    return merged


def _build_subject_locale_map(db: Session, subjects: List[Subject]) -> Dict[str, Dict[str, Subject]]:
    codes = sorted({s.code for s in subjects if getattr(s, "code", None)})
    if not codes:
        return {}

    rows = db.execute(
        select(Subject)
        .where(Subject.code.in_(codes))
        .where(func.lower(func.coalesce(Subject.locale, "")) .in_(["vi", "en"]))
    ).scalars().all()

    locale_map: Dict[str, Dict[str, Subject]] = {}
    for subject in rows:
        code = subject.code
        if not code:
            continue
        locale = (subject.locale or "").lower()
        if locale not in {"vi", "en"}:
            continue
        locale_map.setdefault(code, {})[locale] = subject

    return locale_map


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
        query = query.where(
            or_(
                cast(getattr(model, "vn_name", getattr(model, "name")), Text).ilike(f"%{normalized_search}%"),
                cast(getattr(model, "en_name", getattr(model, "name")), Text).ilike(f"%{normalized_search}%"),
                cast(getattr(model, "name", getattr(model, "vn_name")), Text).ilike(f"%{normalized_search}%"),
            )
        )

    return query


def get_subjects_by_curriculum(
    curricula_id: int,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    offset = (page - 1) * page_size
    normalized_search = (search or "").strip()

    db = SessionLocal()
    try:
        query = (
            select(Subject, CurriculumSubject)
            .join(CurriculumSubject, CurriculumSubject.subject_id == Subject.id)
            .where(CurriculumSubject.curricula_id == curricula_id)
        )

        if normalized_search:
            query = query.where(
                or_(
                    cast(Subject.vn_name, Text).ilike(f"%{normalized_search}%"),
                    cast(Subject.en_name, Text).ilike(f"%{normalized_search}%"),
                    cast(Subject.name, Text).ilike(f"%{normalized_search}%"),
                )
            )

        total = int(db.scalar(select(func.count()).select_from(query.subquery())) or 0)
        rows = db.execute(query.order_by(Subject.id).offset(offset).limit(page_size)).all()
        locale_map = _build_subject_locale_map(db, [subject for subject, _ in rows])

        data = []
        for subject, link in rows:
            base_item = _serialize_item(subject)

            by_code = locale_map.get(subject.code or "", {})
            vi_row = by_code.get("vi")
            en_row = by_code.get("en")

            if vi_row is not None:
                vi_serialized = _serialize_item(vi_row)
                base_item["attributes"] = vi_serialized.get("attributes", base_item.get("attributes", {}))
                base_item["raw"] = vi_serialized.get("raw", base_item.get("raw", {}))

            if en_row is not None:
                en_serialized = _serialize_item(en_row)
                base_item["attribute_en"] = en_serialized.get("attribute_en", base_item.get("attribute_en", {}))
                base_item["raw_en"] = en_serialized.get("raw_en", base_item.get("raw_en", {}))

            base_item["attributes"] = _merge_subject_attributes(base_item["attributes"], link, "vi")
            base_item["attribute_en"] = _merge_subject_attributes(base_item["attribute_en"], link, "en")
            data.append(base_item)

        return {
            "data": data,
            "totalRecords": total,
            "page": page,
            "pageSize": page_size,
            "skippedRecords": offset,
        }
    finally:
        db.close()


def get_subject_from_curriculum(curricula_id: int, subject_id: int) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        row = db.execute(
            select(Subject, CurriculumSubject)
            .join(CurriculumSubject, CurriculumSubject.subject_id == Subject.id)
            .where(CurriculumSubject.curricula_id == curricula_id)
            .where(Subject.id == subject_id)
        ).first()

        if not row:
            return None

        subject, link = row
        base_item = _serialize_item(subject)

        locale_map = _build_subject_locale_map(db, [subject])
        by_code = locale_map.get(subject.code or "", {})
        vi_row = by_code.get("vi")
        en_row = by_code.get("en")

        if vi_row is not None:
            vi_serialized = _serialize_item(vi_row)
            base_item["attributes"] = vi_serialized.get("attributes", base_item.get("attributes", {}))
            base_item["raw"] = vi_serialized.get("raw", base_item.get("raw", {}))

        if en_row is not None:
            en_serialized = _serialize_item(en_row)
            base_item["attribute_en"] = en_serialized.get("attribute_en", base_item.get("attribute_en", {}))
            base_item["raw_en"] = en_serialized.get("raw_en", base_item.get("raw_en", {}))

        base_item["attributes"] = _merge_subject_attributes(base_item["attributes"], link, "vi")
        base_item["attribute_en"] = _merge_subject_attributes(base_item["attribute_en"], link, "en")
        return base_item
    finally:
        db.close()


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

    db = SessionLocal()
    try:
        query = _build_query(table, id=id, filters=filters, search=search)
        total = int(db.scalar(select(func.count()).select_from(query.subquery())) or 0)

        model = TABLE_MODELS[table]
        if table == "schools":
            order_clause = model.id.asc()
        elif table == "curricula":
            order_clause = func.coalesce(model.vn_name, model.name, model.en_name).asc()
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
    finally:
        db.close()


def _map_attrs_to_new_row(table: str, attributes: Dict[str, Any], parent_col: Optional[str], parent_id: Optional[int]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if parent_col and parent_id is not None:
        payload[parent_col] = parent_id

    if table == "schools":
        code = _to_text(attributes.get("schoolCode")) or _to_text(attributes.get("code"))
        payload.update(
            {
                "vn_name": _to_text(attributes.get("name")),
                "vn_slug": _to_text(attributes.get("slug")),
                "vn_locale": _to_text(attributes.get("locale")) or "vi",
                "vn_description": _to_text(attributes.get("description")),
                "vn_code": code,
                "vn_raw_attributes": {"attributes": attributes},
            }
        )
    elif table == "faculties":
        payload.update(
            {
                "vn_name": _to_text(attributes.get("name")),
                "vn_slug": _to_text(attributes.get("slug")),
                "vn_locale": _to_text(attributes.get("locale")) or "vi",
                "vn_description": _to_text(attributes.get("description")),
                "vn_code": _to_text(attributes.get("facultyCode")) or _to_text(attributes.get("code")),
                "vn_raw_attributes": {"attributes": attributes},
            }
        )
    elif table == "majors":
        code = _to_text(attributes.get("majorCode")) or _to_text(attributes.get("code"))
        payload.update(
            {
                "name": _to_text(attributes.get("name")),
                "slug": _to_text(attributes.get("slug")),
                "locale": _to_text(attributes.get("locale")) or "vi",
                "description": _to_text(attributes.get("description")),
                "faculty_code": code,
                "vn_name": _to_text(attributes.get("name")),
                "vn_slug": _to_text(attributes.get("slug")),
                "vn_locale": _to_text(attributes.get("locale")) or "vi",
                "vn_description": _to_text(attributes.get("description")),
                "vn_faculty_code": code,
                "vn_raw_attributes": {"attributes": attributes},
            }
        )
    elif table == "curricula":
        code = _to_text(attributes.get("curriculumCode")) or _to_text(attributes.get("code"))
        payload.update(
            {
                "name": _to_text(attributes.get("name")),
                "slug": _to_text(attributes.get("slug")),
                "locale": _to_text(attributes.get("locale")) or "vi",
                "description": _to_text(attributes.get("description")),
                "code": code,
                "vn_name": _to_text(attributes.get("name")),
                "vn_slug": _to_text(attributes.get("slug")),
                "vn_locale": _to_text(attributes.get("locale")) or "vi",
                "vn_description": _to_text(attributes.get("description")),
                "vn_code": code,
                "vn_raw_attributes": {"attributes": attributes},
            }
        )
    elif table == "subjects":
        code = _to_text(attributes.get("subjectCode")) or _to_text(attributes.get("subCode")) or _to_text(attributes.get("code"))
        payload.update(
            {
                "name": _to_text(attributes.get("name")),
                "slug": _to_text(attributes.get("slug")),
                "locale": _to_text(attributes.get("locale")) or "vi",
                "description": _to_text(attributes.get("description")),
                "short_name": _to_text(attributes.get("shortName")),
                "code": code,
                "vn_name": _to_text(attributes.get("name")),
                "vn_slug": _to_text(attributes.get("slug")),
                "vn_locale": _to_text(attributes.get("locale")) or "vi",
                "vn_description": _to_text(attributes.get("description")),
                "vn_short_name": _to_text(attributes.get("shortName")),
                "vn_code": code,
                "vn_raw_attributes": {"attributes": attributes},
            }
        )

    return payload


def create_item(
    table: str,
    attributes: Dict[str, Any],
    parent_col: Optional[str] = None,
    parent_id: Optional[int] = None,
) -> int:
    _validate_table(table)

    model = TABLE_MODELS[table]
    db = SessionLocal()
    try:
        payload = _map_attrs_to_new_row(table, attributes, parent_col, parent_id)

        item = model(**payload)
        db.add(item)
        db.flush()
        created_id = int(item.id)

        if table == "subjects" and parent_col == "curricula_id" and parent_id is not None:
            db.add(
                CurriculumSubject(
                    curricula_id=parent_id,
                    subject_id=created_id,
                    mandatory=False,
                    link_attributes={},
                )
            )

        db.commit()
        return created_id
    finally:
        db.close()


def update_item(table: str, id: int, attributes: Optional[Dict[str, Any]] = None) -> bool:
    _validate_table(table)
    if attributes is None:
        return False

    model = TABLE_MODELS[table]
    db = SessionLocal()
    try:
        item = db.get(model, id)
        if item is None:
            return False

        payload = _map_attrs_to_new_row(table, attributes, None, None)
        for key, value in payload.items():
            setattr(item, key, value)

        db.commit()
        return True
    finally:
        db.close()


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

    db = SessionLocal()
    try:
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
    finally:
        db.close()


def get_single_item(table: str, id: int) -> Optional[Dict[str, Any]]:
    _validate_table(table)
    model = TABLE_MODELS[table]

    db = SessionLocal()
    try:
        item = db.get(model, id)
        if item is None:
            return None
        return _serialize_item(item)
    finally:
        db.close()


def migrate_subject_links_from_curricula() -> int:
    # Legacy function kept for compatibility; new schema stores links directly.
    return 0


def get_scoped_search_suggestions(keyword: str, scope: str, limit_results: int = 6, parent_filters: dict = None) -> list[dict]:
    normalized_search = f"%{keyword.strip()}%"
    suggestions = []
    if parent_filters is None:
        parent_filters = {}

    db = SessionLocal()
    try:
        if scope == "subjects":
            query = select(Subject.id, func.coalesce(Subject.vn_name, Subject.name, Subject.en_name))
            if parent_filters.get("curricula_id"):
                query = query.join(CurriculumSubject, CurriculumSubject.subject_id == Subject.id)
                query = query.where(CurriculumSubject.curricula_id == int(parent_filters["curricula_id"]))
            query = query.where(
                or_(
                    Subject.vn_name.ilike(normalized_search),
                    Subject.en_name.ilike(normalized_search),
                    Subject.name.ilike(normalized_search),
                )
            ).limit(limit_results)
            rows = db.execute(query).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/syllabus?subject_id={item_id}&curricula_id={parent_filters.get('curricula_id', '')}"})

        elif scope == "majors":
            query = select(Major.id, func.coalesce(Major.vn_name, Major.name, Major.en_name))
            if parent_filters.get("faculty_id"):
                query = query.where(Major.faculty_id == int(parent_filters["faculty_id"]))
            query = query.where(
                or_(
                    Major.vn_name.ilike(normalized_search),
                    Major.en_name.ilike(normalized_search),
                    Major.name.ilike(normalized_search),
                )
            ).limit(limit_results)
            rows = db.execute(query).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/curricula?major_id={item_id}"})

        elif scope == "curricula":
            query = select(Curriculum.id, func.coalesce(Curriculum.vn_name, Curriculum.name, Curriculum.en_name))
            if parent_filters.get("major_id"):
                query = query.where(Curriculum.major_id == int(parent_filters["major_id"]))
            query = query.where(
                or_(
                    Curriculum.vn_name.ilike(normalized_search),
                    Curriculum.en_name.ilike(normalized_search),
                    Curriculum.name.ilike(normalized_search),
                )
            ).limit(limit_results)
            rows = db.execute(query).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/subjects?curricula_id={item_id}"})

        elif scope == "faculties":
            query = select(Faculty.id, func.coalesce(Faculty.vn_name, Faculty.en_name))
            if parent_filters.get("school_id"):
                query = query.where(Faculty.school_id == int(parent_filters["school_id"]))
            query = query.where(or_(Faculty.vn_name.ilike(normalized_search), Faculty.en_name.ilike(normalized_search))).limit(limit_results)
            rows = db.execute(query).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/majors?faculty_id={item_id}"})

        elif scope == "schools":
            query = select(School.id, School.attribute_vn["name"].as_string())
            query = query.where(
                or_(
                    School.attribute_vn["name"].as_string().ilike(normalized_search),
                    School.attribute_en["name"].as_string().ilike(normalized_search),
                )
            ).limit(limit_results)
            rows = db.execute(query).all()
            for item_id, name in rows:
                suggestions.append({"name": name, "url": f"/faculties?school_id={item_id}"})
    finally:
        db.close()
    return suggestions
