# Kien truc Backend va Database

## 1) Tong quan kien truc
Backend duoc xay dung theo huong monolith nhe voi FastAPI, phan tach theo layer:
- Entry point va web/page routing: api.py
- API routing theo resource: backend/routes/
- Truy cap du lieu va serialization: backend/database.py
- ORM model + ket noi session SQLAlchemy: backend/orm.py
- Cac script data migration/maintenance: scripts/

Luong xu ly tong quat:
1. Client goi page route hoac REST route.
2. Route layer nhan request, doc query params, auth/session, language.
3. Data layer truy van Postgres (Supabase) thong qua SQLAlchemy/DB helper.
4. Ket qua duoc normalize theo format attributes/raw va tra ve JSON hoac render template.

## 2) Backend modules chinh

### api.py
- Khoi tao FastAPI app, static mount, Jinja templates.
- Cau hinh SessionMiddleware cho login session.
- Chua:
  - Health check
  - Login/logout
  - Language switch
  - Cac page route render HTML (schools, faculties, majors, curricula, subjects, syllabus)

### backend/routes/
- Chia API endpoint theo domain:
  - schools.py
  - faculties.py
  - majors.py
  - curricula.py
  - subjects.py
  - common.py
- Cac route nay phuc vu du lieu JSON cho frontend/search.

### backend/database.py
- Chua cac ham truy van tong quat:
  - get_table_data
  - get_single_item
  - get_subject_from_curriculum
  - get_scoped_search_suggestions
- Chua logic mapping/serialization dong bo cho 2 ngon ngu.
- Thuc hien build query, filter, paging, search.

### backend/orm.py
- Khai bao model SQLAlchemy map voi bang trong DB.
- Quan ly SessionLocal va Base model.
- Model quan trong trong flow auth: Teacher.

## 3) Kien truc template/frontend server-side
- Template trong templates/
- Base layout trong templates/UI/base.html
- Cac page con:
  - schools.html
  - faculties.html
  - majors.html
  - curricula.html
  - subjects.html
  - syllabus.html
  - login.html
- context quan trong:
  - lang
  - authenticated
  - teacher_code

## 4) Kien truc Database (PostgreSQL / Supabase)

## 4.1 Cac bang domain chinh
- schools_new
- faculties_new
- majors_new
- curricula_new
- subjects_new
- curriculum_subjects (bang lien ket chuong trinh - hoc phan)
- teachers (xac thuc giang vien)

## 4.2 Nguyen tac dat ten cot song ngu
Da theo pattern dong nhat:
- vn_*: du lieu tieng Viet
- en_*: du lieu tieng Anh

Vi du:
- vn_name / en_name
- vn_description / en_description

Data layer se map ve 2 bo truong:
- attributes/raw: ngon ngu hien tai
- attribute_en/raw_en: du lieu tieng Anh

## 4.3 Quan he du lieu tong quan
- School 1-n Faculty
- Faculty 1-n Major
- Major 1-n Curriculum
- Curriculum n-n Subject (qua curriculum_subjects)
- Teacher gan theo cac cap pham vi (school/faculty/major/curricula tuy cau hinh)

## 5) Auth va session
- Login dung teacher_code + password_hash (bcrypt).
- Session luu teacher_code sau khi dang nhap.
- Cac page route yeu cau session hop le, neu khong se redirect /login.
- Logout xoa session.

## 6) Search va paging
- Hien tai da co phan trang cho cac list page.
- Search ho tro theo tung scope bang query params.
- API suggestions duoc gom theo scope de toi uu UX tim kiem.

## 7) De xuat mo rong
- Tach module auth rieng de de bao tri.
- Them middleware RBAC (admin/teacher/viewer) neu can phan quyen sau.
- Chuan hoa migration SQL thanh tung version (Alembic).
- Bo sung index cho cac cot thuong search (code, vn_name, en_name).
