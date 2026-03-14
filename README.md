# NEU Curriculum Fetcher

This repository includes a Python script that fetches curriculum data from NEU's APIs, stores them into Supabase PostgreSQL, and provides both a REST API and a web frontend (using Jinja2 templates) for browsing the curriculum data.

## Project Structure

- [fetch_and_save.py](fetch_and_save.py) — Script to fetch and upsert data from NEU APIs into Supabase PostgreSQL
- [api.py](api.py) — FastAPI main application with both API routes and frontend routes
- [backend/](backend/) — Modular backend with route handlers and ORM models
- [backend/routes/common.py](backend/routes/common.py) — Shared helpers for list/update/delete route behavior
- [templates/](templates/) — Jinja2 HTML templates for frontend
- [static/](static/) — CSS and static assets
- [migrate_to_supabase.py](migrate_to_supabase.py) — Optional one-time migration from legacy SQLite to Supabase
- [requirements.txt](requirements.txt) — Python dependencies

## Database Schema

Tables created: `schools`, `faculties`, `majors`, `curricula`, `subjects`, `curriculum_subjects`.

The `subjects` table stores unique subject entities, while `curriculum_subjects` stores many-to-many links and curriculum-specific subject metadata (semester, requirement, knowledge block, note, etc.).

## Quick Start

1. **Install dependencies:**
```bash
python -m pip install -r requirements.txt
```

2. **Configure Supabase connection:**
```bash
copy .env.example .env
```

Inside `.env` set your connection:
```env
SUPABASE_DB_URL=postgresql://postgres.<project-ref>:<password>@aws-0-xxx.pooler.supabase.com:5432/postgres
```

3. **Fetch and upsert data from NEU APIs:**
```bash
python fetch_and_save.py
```

4. **Run the server:**
```bash
python api.py
```
Or with uvicorn:
```bash
uvicorn api:app --reload
```

5. **Access the application:**
- **Web Frontend:** http://127.0.0.1:8000/
- **API Documentation:** http://127.0.0.1:8000/docs
- **OpenAPI Schema:** http://127.0.0.1:8000/openapi.json
- **Health Check:** http://127.0.0.1:8000/api/health

## Legacy SQLite Migration (Optional)

If you still have an old `database/syllabus.db`, you can copy that data to Supabase PostgreSQL.

1. Install dependencies:
```bash
python -m pip install -r requirements.txt
```

2. Create `.env` from `.env.example`, then fill Supabase DB URL (from Supabase dashboard -> Connect -> Connection string):
```bash
copy .env.example .env
```

Inside `.env`:
```env
SUPABASE_DB_URL=postgresql://postgres.<project-ref>:<password>@aws-0-xxx.pooler.supabase.com:6543/postgres
```

Alternative format is also supported:
```env
user=postgres.<project-ref>
password=<your-password>
host=aws-0-xxx.pooler.supabase.com
port=5432
dbname=postgres
```

3. Run migration:
```bash
python migrate_to_supabase.py --sqlite-db database/syllabus.db --truncate
```

Notes:
- `--truncate` clears target tables before import. Remove this flag if you only want upsert/update behavior.
- The script auto-creates tables/indexes (`schools`, `faculties`, `majors`, `curricula`, `subjects`, `curriculum_subjects`) if they do not exist.
- JSON text columns from SQLite are stored as `jsonb` in PostgreSQL.
- The script auto-loads `.env` and reads `SUPABASE_DB_URL` (also supports aliases `SUPABASE_URL` and `SUPABSE_URL`).
- If `SUPABASE_DB_URL` is not a full URL, the script can assemble connection info from `user/password/host/port/dbname`.

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
- **GET** `/api/health` — Quick health check (API + Supabase connectivity)
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

## Notes

- The script expects Strapi-style responses (the NEU endpoints provided). It stores the API `attributes` JSON and the raw object into normalized tables managed via SQLAlchemy ORM.
- Subjects are linked to curricula via `curriculum_subjects` (many-to-many), not a single `curricula_id` on `subjects`.
- Bulk delete is available via `/api/{resource}/bulk-delete`.
- Runtime backend now uses Supabase/PostgreSQL connection from `.env` (`SUPABASE_DB_URL` or `DATABASE_URL`).
- If you want me to adapt the schema to specific columns (instead of storing attributes JSON), tell me which fields you need and I will update the script.
