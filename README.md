# NEU Curriculum Fetcher

This repository includes a Python script that fetches curriculum data from NEU's APIs, stores them into an SQLite database, and provides both a REST API and a web frontend (using Jinja2 templates) for browsing the curriculum data.

## Project Structure

- [fetch_and_save.py](fetch_and_save.py) — Script to fetch and save data from APIs to SQLite
- [api.py](api.py) — FastAPI main application with both API routes and frontend routes
- [backend/](backend/) — Modular backend with route handlers and models
- [templates/](templates/) — Jinja2 HTML templates for frontend
- [static/](static/) — CSS and static assets
- [database/](database/) — SQLite database file
- [requirements.txt](requirements.txt) — Python dependencies

## Database Schema

Tables created: `schools`, `faculties`, `majors`, `curricula`, `subjects` (with indexes for search performance)

## Quick Start

1. **Install dependencies:**
```bash
python -m pip install -r requirements.txt
```

2. **Fetch and save data from APIs:**
```bash
python fetch_and_save.py --db database/syllabus.db
```

3. **Run the server:**
```bash
python api.py
```
Or with uvicorn:
```bash
uvicorn api:app --reload
```

4. **Access the application:**
   - **Web Frontend:** http://127.0.0.1:8000/
   - **API Documentation:** http://127.0.0.1:8000/docs
   - **OpenAPI Schema:** http://127.0.0.1:8000/openapi.json

## Frontend Navigation

The web interface provides a hierarchical navigation system:

1. **Home** (`/`) - Browse all schools in a table
2. **Faculties** (`/faculties?school_id={id}`) - View faculties for a selected school
3. **Majors** (`/majors?faculty_id={id}`) - View majors for a selected faculty
4. **Curricula** (`/curricula?major_id={id}`) - View curricula for a selected major
5. **Subjects** (`/subjects?curricula_id={id}`) - View subjects for a selected curriculum
6. **Syllabus** (`/syllabus?subject_id={id}`) - View details/syllabus for a selected subject

Each page includes:
- Search/filter functionality
- Pagination with metadata (total, page, per_page)
- Breadcrumb navigation for context
- Responsive table layout

## REST API Endpoints

**Note:** All API endpoints are prefixed with `/api/`

### List & Filter
- **GET** `/api/schools` — List all schools with pagination
- **GET** `/api/faculties` — List faculties (filter with `?school_id={id}`)
- **GET** `/api/majors` — List majors (filter with `?faculty_id={id}`)
- **GET** `/api/curricula` — List curricula (filter with `?major_id={id}`)
- **GET** `/api/subjects` — List subjects (filter with `?curricula_id={id}`)

**Query Parameters:**
- `page` — Page number (default: 1)
- `pageSize` — Items per page (default: 10, max: 100)
- `id` — Get single item by ID (e.g., `?id=25`)
- `search` — Search text in item attributes
- Filter parameters: `school_id`, `faculty_id`, `major_id`, `curricula_id`

### CRUD Operations
- **POST** `/api/schools`, etc. — Create new item
- **PUT** `/api/schools/{id}`, etc. — Update item
- **DELETE** `/api/schools/{id}`, etc. — Delete single item

### Bulk Operations
- **POST** `/api/schools/bulk-delete`, etc. — Bulk delete (send JSON array: `[1,2,3]`)

## Response Format

All API responses follow this format:

```json
{
  "data": [
    {
      "id": 25,
      "attributes": {
        "name": "Trường Công nghệ",
        "description": "...",
        "schoolCode": "TCN"
      },
      "raw": { ... }
    }
  ],
  "totalRecords": 3,
  "page": 1,
  "pageSize": 10,
  "skippedRecords": 0
}
```

## Technologies Used

- **Backend:** FastAPI, Uvicorn, Pydantic
- **Frontend:** Jinja2 templates, HTML/CSS
- **Database:** Supabase PostgreSQL
- **Data Fetching:** Requests library
- **DELETE** `/schools?ids=1,2,3`, etc. — Bulk delete

5. Open the resulting database with DB Browser for SQLite.

Notes

- The script expects Strapi-style responses (the NEU endpoints provided). It stores the API `attributes` JSON and the raw object into normalized tables managed via SQLAlchemy ORM.
- Subjects are linked to curricula via `curriculum_subjects` (many-to-many), not a single `curricula_id` on `subjects`.
- Bulk delete is available via `/api/{resource}/bulk-delete`.
- Runtime backend now uses Supabase/PostgreSQL connection from `.env` (`SUPABASE_DB_URL` or `DATABASE_URL`).
- If you want me to adapt the schema to specific columns (instead of storing attributes JSON), tell me which fields you need and I will update the script.
# LTWeb