# Tài Khoản Giảng Viên / Teacher Accounts

## Credentials

| Mã GV | Username | Mật khẩu | Trường |
|-------|----------|----------|--------|
| GV001 | GV001 | 123456 | Trường Công nghệ |
| GV002 | GV002 | 123456 | Trường Công nghệ |
| GV003 | GV003 | 123456 | Trường Công nghệ |

## Hướng dẫn đăng nhập

1. Truy cập trang chủ: http://localhost:8000/
2. Bạn sẽ được chuyển hướng đến trang đăng nhập: http://localhost:8000/login
3. Nhập **Mã Giảng viên** (ví dụ: `GV001`)
4. Nhập **Mật khẩu** (mặc định: `123456`)
5. Click **Đăng nhập**

## Tạo tài khoản mới

Để tạo giảng viên mới, sử dụng các command:

```bash
# SSH vào database hoặc dùng API backend
# Ví dụ tạo giảng viên GV004
python -c "
from backend.orm import SessionLocal, Teacher
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
db = SessionLocal()
try:
    teacher = Teacher(
        teacher_code='GV004',
        full_name='Nguyễn Văn D',
        password_hash=pwd_context.hash('123456'),
        school_id=25,
        faculty_id=241,
        major_id=773,
        curricula_id=806
    )
    db.add(teacher)
    db.commit()
    print('✅ Tạo GV004 thành công')
finally:
    db.close()
"
```

## Ghi chú

- Mật khẩu mặc định cho tất cả 3 giáo viên là `123456`
- Bất cứ khi nào đăng nhập thành công, session sẽ được tạo trong 24 giờ
- Để đăng xuất: click nút "Đăng xuất" (nếu có) hoặc xóa cookie session
