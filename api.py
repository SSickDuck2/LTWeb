from math import ceil
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy.orm import Session
from passlib.context import CryptContext

from backend.database import check_db_connection, get_single_item, get_subject_from_curriculum, get_table_data, get_scoped_search_suggestions
from backend.routes.schools import router as schools_router
from backend.routes.faculties import router as faculties_router
from backend.routes.majors import router as majors_router
from backend.routes.curricula import router as curricula_router
from backend.routes.subjects import router as subjects_router
from backend.orm import Teacher, SessionLocal
from backend.database import get_db

app = FastAPI(title="NEU Curriculum API", version="1.0.0")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Add session middleware for authentication
app.add_middleware(SessionMiddleware, secret_key="neu-curriculum-secret-key-2024")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def _template_response(
    name: str,
    request: Request,
    context: Optional[Dict[str, Any]] = None,
    status_code: int = 200,
):
    payload = dict(context or {})
    payload.setdefault("request", request)
    try:
        # Starlette/FastAPI newer signature
        return templates.TemplateResponse(
            request=request,
            name=name,
            context=payload,
            status_code=status_code,
        )
    except TypeError:
        # Backward compatibility with older signature
        return templates.TemplateResponse(name, payload, status_code=status_code)

# API routes
app.include_router(schools_router, prefix="/api")
app.include_router(faculties_router, prefix="/api")
app.include_router(majors_router, prefix="/api")
app.include_router(curricula_router, prefix="/api")
app.include_router(subjects_router, prefix="/api")


@app.get("/api/health")
def health_check():
    try:
        db_state = check_db_connection()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {exc}") from exc

    return {
        "status": "ok",
        "service": "neu-curriculum-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "db": db_state,
    }


# ============================================================================
# Authentication Endpoints
# ============================================================================

def _get_current_teacher(request: Request) -> Optional[str]:
    """Lấy mã giáo viên từ session, nếu đã đăng nhập"""
    return request.session.get("teacher_code")


def _verify_teacher_password(teacher_code: str, password: str) -> bool:
    """Kiểm tra mật khẩu giáo viên"""
    db = SessionLocal()
    try:
        teacher = db.query(Teacher).filter(Teacher.teacher_code == teacher_code).first()
        if not teacher:
            return False
        return pwd_context.verify(password, teacher.password_hash)
    finally:
        db.close()


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    """Trang đăng nhập"""
    # Nếu đã đăng nhập, chuyển hướng về trang chủ
    if _get_current_teacher(request):
        return RedirectResponse(url="/", status_code=303)
    
    lang = _get_lang_from_request(request)
    error = request.query_params.get("error", "")
    
    return _template_response(
        "login.html",
        request,
        {
            "lang": lang,
            "error": error,
        },
    )


@app.post("/login", response_class=HTMLResponse)
def login(request: Request, teacher_code: str = Form(...), password: str = Form(...)):
    """Xử lý đăng nhập"""
    # Kiểm tra username/password
    if not _verify_teacher_password(teacher_code, password):
        lang = _get_lang_from_request(request)
        error_msg = "Mã giảng viên hoặc mật khẩu không chính xác" if lang == "vi" else "Invalid Teacher ID or password"
        return _template_response(
            "login.html",
            request,
            {
                "lang": lang,
                "error": error_msg,
            },
        )
    
    # Lưu session
    request.session["teacher_code"] = teacher_code
    
    db = SessionLocal()
    try:
        teacher = db.query(Teacher).filter(Teacher.teacher_code == teacher_code).first()
        if teacher:
            request.session["teacher_id"] = teacher.teacher_code
    finally:
        db.close()
    
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    """Đăng xuất"""
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/set-lang")
def set_language(lang: str, next_url: str = "/"):
    """Lưu lựa chọn ngôn ngữ vào Cookie và chuyển hướng lại trang cũ"""
    safe_lang = (lang or "vi").lower()
    if safe_lang not in {"vi", "en"}:
        safe_lang = "vi"

    safe_next_url = next_url if next_url.startswith("/") else "/"
    response = RedirectResponse(url=safe_next_url)
    response.set_cookie(key="lang", value=safe_lang, max_age=31536000, path="/") # Lưu 1 năm
    return response


def _get_lang_from_request(request: Request) -> str:
    lang = (request.cookies.get("lang") or "vi").lower()
    return lang if lang in {"vi", "en"} else "vi"

def _apply_language(item: dict, lang: str):
    """Hàm áp dụng ngôn ngữ cho 1 item, nếu không có sẽ chèn câu báo lỗi"""
    if not item: return item
    
    attr_vn = item.get("attributes") or {}
    attr_en = item.get("attribute_en") or {}
    
    if lang == "en":
        target = attr_en
        msg = "(Chưa có phiên bản tiếng Anh của mục này)"
    else:
        target = attr_vn
        msg = "(Chưa có phiên bản tiếng Việt của mục này)"
        
    if not target or not target.get("name"):
        item["attributes"] = {
            "name": msg,
            "schoolCode": attr_vn.get("schoolCode", "N/A"),
            "facultyCode": attr_vn.get("facultyCode", "N/A"),
            "subjectCode": attr_vn.get("subjectCode", attr_vn.get("subCode", "N/A")),
            "subCode": attr_vn.get("subCode", "N/A"),
            "credits": attr_vn.get("credits", "0"),
            "type": attr_vn.get("type", "N/A"),
            "description": msg,
            "objectives": msg,
            "content": msg,
            "prerequisite": msg,
            "assessment": msg,
            "textbook": msg,
        }
    else:
        item["attributes"] = target
    return item

def _resolve_name(table: str, item_id: Optional[int], lang: str = "vi") -> str:
    """Lấy tên theo ngôn ngữ đang chọn"""
    if item_id is None:
        return "Unknown"
    item = get_single_item(table, item_id)
    if not item:
        return "Unknown"
        
    attr_vn = item.get("attributes", {})
    attr_en = item.get("attribute_en", {})
    
    target = attr_en if lang == "en" else attr_vn
    if not target or not target.get("name"):
        return "(Chưa có bản tiếng Anh)" if lang == "en" else "(Chưa có bản tiếng Việt)"
    return target.get("name", "Unknown")

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
    # Check authentication
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        data = get_table_data("schools", page=page, page_size=10, search=search)
        schools = [_apply_language(item, lang) for item in data.get("data", [])]

        return _template_response(
            "schools.html",
            request,
            {
                "schools": schools,
                "meta": _build_meta(data, 10),
                "search": search,
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
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
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        school_name = _resolve_name("schools", school_id, lang)
        data = get_table_data(
            "faculties",
            page=page,
            page_size=10,
            filters={"school_id": school_id},
            search=search,
        )
        faculties = [_apply_language(item, lang) for item in data.get("data", [])]

        return _template_response(
            "faculties.html",
            request,
            {
                "faculties": faculties,
                "meta": _build_meta(data, 10),
                "school_id": school_id,
                "school_name": school_name,
                "search": search,
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
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
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        school_id_int = _parse_optional_id(school_id)
        faculty_name = _resolve_name("faculties", faculty_id, lang)
        school_name = _resolve_name("schools", school_id_int, lang)
        data = get_table_data(
            "majors",
            page=page,
            page_size=10,
            filters={"faculty_id": faculty_id},
            search=search,
        )
        majors = [_apply_language(item, lang) for item in data.get("data", [])]

        return _template_response(
            "majors.html",
            request,
            {
                "majors": majors,
                "meta": _build_meta(data, 10),
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
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
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        major_name = _resolve_name("majors", major_id, lang)
        faculty_name = _resolve_name("faculties", faculty_id_int, lang)
        school_name = _resolve_name("schools", school_id_int, lang)
        data = get_table_data(
            "curricula",
            page=page,
            page_size=10,
            filters={"major_id": major_id},
            search=search,
        )
        curricula = [_apply_language(item, lang) for item in data.get("data", [])]

        return _template_response(
            "curricula.html",
            request,
            {
                "curricula": curricula,
                "meta": _build_meta(data, 10),
                "major_id": major_id,
                "major_name": major_name,
                "faculty_id": faculty_id_int,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
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
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        major_id_int = _parse_optional_id(major_id)
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        curriculum_name = _resolve_name("curricula", curricula_id, lang)
        major_name = _resolve_name("majors", major_id_int, lang)
        faculty_name = _resolve_name("faculties", faculty_id_int, lang)
        school_name = _resolve_name("schools", school_id_int, lang)

        current_page_size = 1000 if search else 20

        data = get_table_data(
            "subjects",
            page=page,
            page_size=current_page_size,
            filters={"curricula_id": curricula_id},
            search=search,
        )
        subjects = [_apply_language(item, lang) for item in data.get("data", [])]

        return _template_response(
            "subjects.html",
            request,
            {
                "subjects": subjects,
                "meta": _build_meta(data, current_page_size),
                "curricula_id": curricula_id,
                "curriculum_name": curriculum_name,
                "major_id": major_id_int,
                "major_name": major_name,
                "faculty_id": faculty_id_int,
                "faculty_name": faculty_name,
                "school_id": school_id_int,
                "school_name": school_name,
                "search": search,
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


import json
import os

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
    teacher_code = _get_current_teacher(request)
    if not teacher_code:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        lang = _get_lang_from_request(request)
        curricula_id_int = _parse_optional_id(curricula_id)
        major_id_int = _parse_optional_id(major_id)
        faculty_id_int = _parse_optional_id(faculty_id)
        school_id_int = _parse_optional_id(school_id)
        
        subject = None
        if curricula_id_int is not None:
            subject = get_subject_from_curriculum(curricula_id_int, subject_id)
        if not subject:
            subject = get_single_item("subjects", subject_id)
        if subject:
            subject = _apply_language(subject, lang)
        subject_name = subject.get("attributes", {}).get("name", "Unknown") if subject else "Unknown"
        curriculum_name = _resolve_name("curricula", curricula_id_int, lang)
        major_name = _resolve_name("majors", major_id_int, lang)
        faculty_name = _resolve_name("faculties", faculty_id_int, lang)
        school_name = _resolve_name("schools", school_id_int, lang)

        syllabus_detail = None
        try:
            with open("detailSyllabus.json", "r", encoding="utf-8") as f:
                syllabus_detail = json.load(f)
        except Exception as e:
            print(f"Không thể đọc file JSON: {e}")

        return templates.TemplateResponse(
            "syllabus.html",
            request,
            {
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
                "lang": lang,
                "authenticated": True,
                "teacher_code": teacher_code,
            },
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"
    
@app.get("/api/search/suggestions")
def search_suggestions_api(
    request: Request, # Thêm tham số request để đọc URL
    q: str = Query(..., min_length=2), 
    scope: str = Query(...)
):
    try:
        # Tự động quét xem người dùng đang ở trang có ID gì
        parent_filters = {}
        for key in ["curricula_id", "major_id", "faculty_id", "school_id"]:
            val = request.query_params.get(key)
            if val:
                parent_filters[key] = val
                
        # Truyền bộ lọc vào database
        results = get_scoped_search_suggestions(keyword=q, scope=scope, parent_filters=parent_filters)
        return {"status": "success", "data": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/siblings")
def get_siblings_api(
    request: Request,
    table: str = Query(...),
    parent_col: Optional[str] = Query(None),
    parent_id: Optional[int] = Query(None)
):
    """API tải danh sách các mục con để hiển thị trong Dropdown Breadcrumb"""
    try:
        lang = _get_lang_from_request(request)
        filters = {parent_col: parent_id} if parent_col and parent_id else None
        
        # Lấy tối đa 100 mục con
        data = get_table_data(table, page=1, page_size=100, filters=filters)
        items = [_apply_language(item, lang) for item in data.get("data", [])]
        
        # Chỉ trả về ID và Tên cho nhẹ
        result = [{"id": item["id"], "name": item["attributes"]["name"]} for item in items]
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
