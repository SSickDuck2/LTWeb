# Session Notes - 2026-03-29

## Muc tieu session
- Hoan thien migration schema schools sang schools_new theo chuan song ngu vn/en.
- Bo sung he thong dang nhap bat buoc truoc khi vao cac trang giao dien.
- Tao file thong tin tai khoan 3 giang vien de test.

## Cac thay doi chinh da thuc hien

### 1. Migration schema schools
- Da cap nhat mapping va serializer de doc du lieu tu schema schools_new.
- Da dam bao output giu dung contract cu:
  - id
  - attributes / raw (ngu canh hien tai)
  - attribute_en / raw_en (ngu canh tieng Anh)

### 2. Authentication cho giao dien
- Da bo sung SessionMiddleware trong FastAPI app.
- Da them helper lay thong tin giao vien dang dang nhap tu session.
- Da them logic verify mat khau bang passlib bcrypt voi bang Teacher.
- Da them endpoint dang nhap:
  - GET /login: hien thi form dang nhap
  - POST /login: xac thuc teacher_code + password, set session
- Da them endpoint dang xuat:
  - logout xoa session va quay ve trang login
- Da dat guard cho cac page route (neu chua dang nhap thi redirect ve /login):
  - /
  - /faculties
  - /majors
  - /curricula
  - /subjects
  - /syllabus

### 3. Cap nhat template giao dien
- Da doi navbar tai UI base template:
  - Neu da dang nhap: hien thi menu Tai khoan va nut Dang xuat
  - Neu chua dang nhap: hien thi nut Dang nhap
- Da truyen them context cho template:
  - authenticated
  - teacher_code

### 4. Tai khoan test
- Da tao file CREDENTIALS.md o root repo.
- Danh sach tai khoan:
  - GV001 / 123456
  - GV002 / 123456
  - GV003 / 123456

## Dependency va van hanh
- Trong qua trinh chay app, da gap loi thieu module itsdangerous (yeu cau cho session).
- Da cai bo sung itsdangerous vao moi truong virtualenv de app chay duoc.

## Ket qua test trong session
- Dang nhap thanh cong voi tai khoan GV001 va GV002.
- User chua dang nhap bi redirect dung sang /login.
- Dang xuat thanh cong, session bi xoa.
- Co the truy cap tiep cac trang protected sau khi dang nhap.

## Ghi chu
- Nên bo sung itsdangerous vao requirements.txt de tranh loi khi setup moi truong moi.
- Co the tach auth thanh module rieng (backend/routes/auth.py + dependency) neu muon mo rong RBAC sau nay.
