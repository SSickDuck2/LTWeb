from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
import requests
import os

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

# Frontend routes
@app.get("/", response_class=HTMLResponse)
def home(request: Request, page: int = Query(1, ge=1), search: str = Query(None)):
    """Home page - list of schools"""
    try:
        params = {"page": page, "pageSize": 10}
        response = requests.get("http://127.0.0.1:8000/api/schools", params=params)
        data = response.json()
        
        return templates.TemplateResponse(
            "schools.html",
            {
                "request": request,
                "schools": data.get("data", []),
                "meta": {
                    "page": data.get("page", 1),
                    "pageSize": data.get("pageSize", 10),
                    "total": data.get("totalRecords", 0)
                },
                "search": search
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/faculties", response_class=HTMLResponse)
def faculties_page(request: Request, school_id: int = Query(...), page: int = Query(1, ge=1), search: str = Query(None)):
    """Faculties page"""
    try:
        # Get school info
        school_response = requests.get(f"http://127.0.0.1:8000/api/schools?id={school_id}")
        school_data = school_response.json()
        school_name = school_data["data"][0]["attributes"]["name"] if school_data.get("data") else "Unknown"
        
        # Get faculties
        params = {"page": page, "pageSize": 10, "school_id": school_id}
        response = requests.get("http://127.0.0.1:8000/api/faculties", params=params)
        data = response.json()
        
        return templates.TemplateResponse(
            "faculties.html",
            {
                "request": request,
                "faculties": data.get("data", []),
                "meta": {
                    "page": data.get("page", 1),
                    "pageSize": data.get("pageSize", 10),
                    "total": data.get("totalRecords", 0)
                },
                "school_id": school_id,
                "school_name": school_name,
                "search": search
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/majors", response_class=HTMLResponse)
def majors_page(request: Request, faculty_id: int = Query(...), school_id: int = Query(None), page: int = Query(1, ge=1), search: str = Query(None)):
    """Majors page"""
    try:
        # Get school and faculty info
        faculty_response = requests.get(f"http://127.0.0.1:8000/api/faculties?id={faculty_id}")
        faculty_data = faculty_response.json()
        faculty_name = faculty_data["data"][0]["attributes"]["name"] if faculty_data.get("data") else "Unknown"
        
        # Get majors
        params = {"page": page, "pageSize": 10, "faculty_id": faculty_id}
        response = requests.get("http://127.0.0.1:8000/api/majors", params=params)
        data = response.json()
        
        # Get school name if not provided
        school_name = "Unknown"
        if school_id:
            school_response = requests.get(f"http://127.0.0.1:8000/api/schools?id={school_id}")
            school_data = school_response.json()
            school_name = school_data["data"][0]["attributes"]["name"] if school_data.get("data") else "Unknown"
        
        return templates.TemplateResponse(
            "majors.html",
            {
                "request": request,
                "majors": data.get("data", []),
                "meta": {
                    "page": data.get("page", 1),
                    "pageSize": data.get("pageSize", 10),
                    "total": data.get("totalRecords", 0)
                },
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id,
                "school_name": school_name,
                "search": search
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/curricula", response_class=HTMLResponse)
def curricula_page(request: Request, major_id: int = Query(...), faculty_id: int = Query(None), school_id: int = Query(None), page: int = Query(1, ge=1), search: str = Query(None)):
    """Curricula page"""
    try:
        # Get major info
        major_response = requests.get(f"http://127.0.0.1:8000/api/majors?id={major_id}")
        major_data = major_response.json()
        major_name = major_data["data"][0]["attributes"]["name"] if major_data.get("data") else "Unknown"
        
        # Get curricula
        params = {"page": page, "pageSize": 10, "major_id": major_id}
        response = requests.get("http://127.0.0.1:8000/api/curricula", params=params)
        data = response.json()
        
        # Get faculty and school names
        faculty_name = "Unknown"
        if faculty_id:
            faculty_response = requests.get(f"http://127.0.0.1:8000/api/faculties?id={faculty_id}")
            faculty_data = faculty_response.json()
            faculty_name = faculty_data["data"][0]["attributes"]["name"] if faculty_data.get("data") else "Unknown"
        
        school_name = "Unknown"
        if school_id:
            school_response = requests.get(f"http://127.0.0.1:8000/api/schools?id={school_id}")
            school_data = school_response.json()
            school_name = school_data["data"][0]["attributes"]["name"] if school_data.get("data") else "Unknown"
        
        return templates.TemplateResponse(
            "curricula.html",
            {
                "request": request,
                "curricula": data.get("data", []),
                "meta": {
                    "page": data.get("page", 1),
                    "pageSize": data.get("pageSize", 10),
                    "total": data.get("totalRecords", 0)
                },
                "major_id": major_id,
                "major_name": major_name,
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id,
                "school_name": school_name,
                "search": search
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/subjects", response_class=HTMLResponse)
def subjects_page(request: Request, curricula_id: int = Query(...), major_id: int = Query(None), faculty_id: int = Query(None), school_id: int = Query(None), page: int = Query(1, ge=1), search: str = Query(None)):
    """Subjects page"""
    try:
        # Get curriculum info
        curriculum_response = requests.get(f"http://127.0.0.1:8000/api/curricula?id={curricula_id}")
        curriculum_data = curriculum_response.json()
        curriculum_name = curriculum_data["data"][0]["attributes"]["name"] if curriculum_data.get("data") else "Unknown"
        
        # Get subjects
        params = {"page": page, "pageSize": 20, "curricula_id": curricula_id}
        response = requests.get("http://127.0.0.1:8000/api/subjects", params=params)
        data = response.json()
        
        # Get other names
        major_name = "Unknown"
        if major_id:
            major_response = requests.get(f"http://127.0.0.1:8000/api/majors?id={major_id}")
            major_data = major_response.json()
            major_name = major_data["data"][0]["attributes"]["name"] if major_data.get("data") else "Unknown"
        
        faculty_name = "Unknown"
        if faculty_id:
            faculty_response = requests.get(f"http://127.0.0.1:8000/api/faculties?id={faculty_id}")
            faculty_data = faculty_response.json()
            faculty_name = faculty_data["data"][0]["attributes"]["name"] if faculty_data.get("data") else "Unknown"
        
        school_name = "Unknown"
        if school_id:
            school_response = requests.get(f"http://127.0.0.1:8000/api/schools?id={school_id}")
            school_data = school_response.json()
            school_name = school_data["data"][0]["attributes"]["name"] if school_data.get("data") else "Unknown"
        
        return templates.TemplateResponse(
            "subjects.html",
            {
                "request": request,
                "subjects": data.get("data", []),
                "meta": {
                    "page": data.get("page", 1),
                    "pageSize": data.get("pageSize", 20),
                    "total": data.get("totalRecords", 0)
                },
                "curricula_id": curricula_id,
                "curriculum_name": curriculum_name,
                "major_id": major_id,
                "major_name": major_name,
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id,
                "school_name": school_name,
                "search": search
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.get("/syllabus", response_class=HTMLResponse)
def syllabus_page(request: Request, subject_id: int = Query(...), curricula_id: int = Query(None), major_id: int = Query(None), faculty_id: int = Query(None), school_id: int = Query(None)):
    """Subject details/syllabus page"""
    try:
        # Get subject info
        subject_response = requests.get(f"http://127.0.0.1:8000/api/subjects?id={subject_id}")
        subject_data = subject_response.json()
        subject = subject_data["data"][0] if subject_data.get("data") else None
        subject_name = subject["attributes"]["name"] if subject else "Unknown"
        
        # Get other names
        curriculum_name = "Unknown"
        if curricula_id:
            curriculum_response = requests.get(f"http://127.0.0.1:8000/api/curricula?id={curricula_id}")
            curriculum_data = curriculum_response.json()
            curriculum_name = curriculum_data["data"][0]["attributes"]["name"] if curriculum_data.get("data") else "Unknown"
        
        major_name = "Unknown"
        if major_id:
            major_response = requests.get(f"http://127.0.0.1:8000/api/majors?id={major_id}")
            major_data = major_response.json()
            major_name = major_data["data"][0]["attributes"]["name"] if major_data.get("data") else "Unknown"
        
        faculty_name = "Unknown"
        if faculty_id:
            faculty_response = requests.get(f"http://127.0.0.1:8000/api/faculties?id={faculty_id}")
            faculty_data = faculty_response.json()
            faculty_name = faculty_data["data"][0]["attributes"]["name"] if faculty_data.get("data") else "Unknown"
        
        school_name = "Unknown"
        if school_id:
            school_response = requests.get(f"http://127.0.0.1:8000/api/schools?id={school_id}")
            school_data = school_response.json()
            school_name = school_data["data"][0]["attributes"]["name"] if school_data.get("data") else "Unknown"
        
        return templates.TemplateResponse(
            "syllabus.html",
            {
                "request": request,
                "subject": subject,
                "subject_name": subject_name,
                "curricula_id": curricula_id,
                "curriculum_name": curriculum_name,
                "major_id": major_id,
                "major_name": major_name,
                "faculty_id": faculty_id,
                "faculty_name": faculty_name,
                "school_id": school_id,
                "school_name": school_name
            }
        )
    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
