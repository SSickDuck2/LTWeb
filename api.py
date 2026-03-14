from math import ceil
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from backend.database import get_single_item, get_subject_from_curriculum, get_table_data
from backend.routes.schools import router as schools_router
from backend.routes.faculties import router as faculties_router
from backend.routes.majors import router as majors_router
from backend.routes.curricula import router as curricula_router
from backend.routes.subjects import router as subjects_router

app = FastAPI(title="NEU Curriculum API", version="1.0.0")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# API routes
app.include_router(schools_router, prefix="/api")
app.include_router(faculties_router, prefix="/api")
app.include_router(majors_router, prefix="/api")
app.include_router(curricula_router, prefix="/api")
app.include_router(subjects_router, prefix="/api")


def _build_meta(payload: Dict[str, Any], page_size: int) -> Dict[str, Any]:
    resolved_page = payload.get("page", 1)
    resolved_page_size = payload.get("pageSize", page_size)
    total = payload.get("totalRecords", 0)
    total_pages = max(1, ceil(total / resolved_page_size)) if resolved_page_size else 1

    return {
        "page": resolved_page,
        "pageSize": resolved_page_size,
        "total": total,
        "totalPages": total_pages,
        "hasPrev": resolved_page > 1,
        "hasNext": resolved_page < total_pages,
    }


def _resolve_name(table: str, item_id: Optional[int]) -> str:
    if item_id is None:
        return "Unknown"
    item = get_single_item(table, item_id)
    if not item:
        return "Unknown"
    return item.get("attributes", {}).get("name", "Unknown")


def _parse_optional_id(value: Optional[str]) -> Optional[int]:
    # Keep old bookmarked URLs like "school_id=None" from breaking request validation.
    if value in (None, "", "None", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

# Frontend routes
@app.get("/", response_class=HTMLResponse)
def home(request: Request, page: int = Query(1, ge=1), search: Optional[str] = Query(None)):
    """Home page - list of schools"""
    try:
        data = get_table_data("schools", page=page, page_size=10, search=search)

        return templates.TemplateResponse(
            "schools.html",
            {
                "request": request,
                "schools": data.get("data", []),
                "meta": _build_meta(data, 10),
                "search": search,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/faculties", response_class=HTMLResponse)
def faculties_page(
    request: Request,
    school_id: int = Query(...),
    page: int = Query(1, ge=1),
    search: Optional[str] = Query(None),
):
    """Faculties page"""
    try:
        school_name = _resolve_name("schools", school_id)
        data = get_table_data(
            "faculties",
            page=page,
            page_size=10,
            filters={"school_id": school_id},
            search=search,
        )

        return templates.TemplateResponse(
            "faculties.html",
            {
                "request": request,
                "faculties": data.get("data", []),
                "meta": _build_meta(data, 10),
                "school_id": school_id,
                "school_name": school_name,
                "search": search,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/majors", response_class=HTMLResponse)
def majors_page(
    request: Request,
    faculty_id: int = Query(...),
    school_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    search: Optional[str] = Query(None),
):
    """Majors page"""
    try:
        school_id_int = _parse_optional_id(school_id)
        faculty_name = _resolve_name("faculties", faculty_id)
        school_name = _resolve_name("schools", school_id_int)
        data = get_table_data(
            "majors",
            page=page,
            page_size=10,
            filters={"faculty_id": faculty_id},
            search=search,
        )

        return templates.TemplateResponse(
            "majors.html",
            {
                "request": request,
                "majors": data.get("data", []),
                "meta": _build_meta(data, 10),
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/curricula", response_class=HTMLResponse)
def curricula_page(
    request: Request,
    major_id: int = Query(...),
    faculty_id: Optional[str] = Query(None),
    school_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    search: Optional[str] = Query(None),
):
    """Curricula page"""
    try:
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        major_name = _resolve_name("majors", major_id)
        faculty_name = _resolve_name("faculties", faculty_id_int)
        school_name = _resolve_name("schools", school_id_int)
        data = get_table_data(
            "curricula",
            page=page,
            page_size=10,
            filters={"major_id": major_id},
            search=search,
        )

        return templates.TemplateResponse(
            "curricula.html",
            {
                "request": request,
                "curricula": data.get("data", []),
                "meta": _build_meta(data, 10),
                "major_id": major_id,
                "major_name": major_name,
                "faculty_id": faculty_id_int,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/subjects", response_class=HTMLResponse)
def subjects_page(
    request: Request,
    curricula_id: int = Query(...),
    major_id: Optional[str] = Query(None),
    faculty_id: Optional[str] = Query(None),
    school_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    search: Optional[str] = Query(None),
):
    """Subjects page"""
    try:
        major_id_int = _parse_optional_id(major_id)
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        curriculum_name = _resolve_name("curricula", curricula_id)
        major_name = _resolve_name("majors", major_id_int)
        faculty_name = _resolve_name("faculties", faculty_id_int)
        school_name = _resolve_name("schools", school_id_int)
        data = get_table_data(
            "subjects",
            page=page,
            page_size=20,
            filters={"curricula_id": curricula_id},
            search=search,
        )

        return templates.TemplateResponse(
            "subjects.html",
            {
                "request": request,
                "subjects": data.get("data", []),
                "meta": _build_meta(data, 20),
                "curricula_id": curricula_id,
                "curriculum_name": curriculum_name,
                "major_id": major_id_int,
                "major_name": major_name,
                "faculty_id": faculty_id_int,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/syllabus", response_class=HTMLResponse)
def syllabus_page(
    request: Request,
    subject_id: int = Query(...),
    curricula_id: Optional[str] = Query(None),
    major_id: Optional[str] = Query(None),
    faculty_id: Optional[str] = Query(None),
    school_id: Optional[str] = Query(None),
):
    """Subject details/syllabus page"""
    try:
        curricula_id_int = _parse_optional_id(curricula_id)
        major_id_int = _parse_optional_id(major_id)
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        subject = None
        if curricula_id_int is not None:
            subject = get_subject_from_curriculum(curricula_id_int, subject_id)
        if not subject:
            subject = get_single_item("subjects", subject_id)
        subject_name = subject.get("attributes", {}).get("name", "Unknown") if subject else "Unknown"
        curriculum_name = _resolve_name("curricula", curricula_id_int)
        major_name = _resolve_name("majors", major_id_int)
        faculty_name = _resolve_name("faculties", faculty_id_int)
        school_name = _resolve_name("schools", school_id_int)

        return templates.TemplateResponse(
            "syllabus.html",
            {
                "request": request,
                "subject": subject,
                "subject_name": subject_name,
                "curricula_id": curricula_id_int,
                "curriculum_name": curriculum_name,
                "major_id": major_id_int,
                "major_name": major_name,
                "faculty_id": faculty_id_int,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
