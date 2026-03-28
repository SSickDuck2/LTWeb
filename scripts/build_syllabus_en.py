import sqlite3
import requests
import json
import os
import time

# ==========================================
# CẤU HÌNH API (BẢN TIẾNG ANH - EN)
# ==========================================
SCHOOLS_URL = "https://courses.neu.edu.vn/backend/api/curriculum-schools?populate=curriculum_faculties&locale=en"
FACULTIES_URL = "https://courses.neu.edu.vn/backend/api/curriculum-faculties?populate=curriculum_majors&pagination[withCount]=false&pagination[pageSize]=500&locale=en"
MAJORS_URL = "https://courses.neu.edu.vn/backend/api/curriculum-majors?filters[active][$eq]=true&pagination[withCount]=false&pagination[pageSize]=2000&populate=curriculum_curricula&sort[]=admissionCode:asc&locale=en"
SUBJECTS_URL = "https://courses.neu.edu.vn/backend/api/curriculum-majors?populate[curriculum_curricula][populate][curriculum_curriculum_subjects][populate][curriculum_subject]=*&pagination[withCount]=false&pagination[pageSize]=70&locale=en"

# ==========================================
# CẤU HÌNH DATABASE
# ==========================================
DB_DIR = "database"
DB_PATH = os.path.join(DB_DIR, "syllabus_en.db") # Xuất ra database tiếng Anh

def setup_database():
    """Tạo thư mục và khởi tạo cấu trúc các bảng SQLite"""
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
        
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"[*] Đã xóa database cũ: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS schools (id INTEGER PRIMARY KEY, attributes TEXT, raw TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS faculties (id INTEGER PRIMARY KEY, school_id INTEGER, attributes TEXT, raw TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS majors (id INTEGER PRIMARY KEY, faculty_id INTEGER, attributes TEXT, raw TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS curricula (id INTEGER PRIMARY KEY, major_id INTEGER, attributes TEXT, raw TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS subjects (id INTEGER PRIMARY KEY, curricula_id INTEGER, attributes TEXT, raw TEXT)')

    conn.commit()
    return conn

def fetch_with_retry(url, max_retries=3):
    """Hàm tải dữ liệu an toàn có cơ chế thử lại (retry) nếu mạng lỗi"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[!] Lỗi kết nối ở lần thử {attempt + 1}/{max_retries}: {e}")
            time.sleep(2)
    print(f"[X] Thất bại khi lấy dữ liệu từ: {url}")
    return None

def fetch_and_save():
    conn = setup_database()
    cur = conn.cursor()

    # Dùng Dictionary để map ID (Tạo khóa ngoại)
    fac_to_school = {}
    maj_to_fac = {}

    # 1. FETCH SCHOOLS
    print("\n[1/4] Đang lấy dữ liệu Schools (EN)...")
    data_schools = fetch_with_retry(SCHOOLS_URL)
    if data_schools and 'data' in data_schools:
        for item in data_schools['data']:
            s_id = item['id']
            attrs = item['attributes']
            
            # Map Faculty -> School
            if 'curriculum_faculties' in attrs and attrs['curriculum_faculties'].get('data'):
                for fac in attrs['curriculum_faculties']['data']:
                    fac_to_school[fac['id']] = s_id

            cur.execute("INSERT OR REPLACE INTO schools (id, attributes, raw) VALUES (?, ?, ?)", 
                        (s_id, json.dumps(attrs, ensure_ascii=False), json.dumps(item, ensure_ascii=False)))
    conn.commit()

    # 2. FETCH FACULTIES
    print("[2/4] Đang lấy dữ liệu Faculties (EN)...")
    data_faculties = fetch_with_retry(FACULTIES_URL)
    if data_faculties and 'data' in data_faculties:
        for item in data_faculties['data']:
            f_id = item['id']
            attrs = item['attributes']
            s_id = fac_to_school.get(f_id)
            
            # Map Major -> Faculty
            if 'curriculum_majors' in attrs and attrs['curriculum_majors'].get('data'):
                for maj in attrs['curriculum_majors']['data']:
                    maj_to_fac[maj['id']] = f_id

            cur.execute("INSERT OR REPLACE INTO faculties (id, school_id, attributes, raw) VALUES (?, ?, ?, ?)", 
                        (f_id, s_id, json.dumps(attrs, ensure_ascii=False), json.dumps(item, ensure_ascii=False)))
    conn.commit()

    # 3. FETCH MAJORS
    print("[3/4] Đang lấy dữ liệu Majors (EN)...")
    data_majors = fetch_with_retry(MAJORS_URL)
    if data_majors and 'data' in data_majors:
        for item in data_majors['data']:
            m_id = item['id']
            attrs = item['attributes']
            f_id = maj_to_fac.get(m_id)

            cur.execute("INSERT OR REPLACE INTO majors (id, faculty_id, attributes, raw) VALUES (?, ?, ?, ?)", 
                        (m_id, f_id, json.dumps(attrs, ensure_ascii=False), json.dumps(item, ensure_ascii=False)))
    conn.commit()

    # 4. FETCH CURRICULA & SUBJECTS (Có phân trang)
    print("[4/4] Đang lấy dữ liệu Curricula & Subjects (EN)...")
    page = 1
    has_more = True
    total_subjects_saved = 0
    total_curricula_saved = 0

    while has_more:
        print(f"  -> Đang quét trang {page}...")
        url = f"{SUBJECTS_URL}&pagination[page]={page}"
        data_subjects = fetch_with_retry(url)
        
        if not data_subjects or not data_subjects.get('data'):
            break

        for major_item in data_subjects['data']:
            m_id = major_item['id']
            attrs = major_item['attributes']

            if 'curriculum_curricula' in attrs and attrs['curriculum_curricula'].get('data'):
                for curr_item in attrs['curriculum_curricula']['data']:
                    c_id = curr_item['id']
                    c_attrs = curr_item['attributes']

                    # Insert Curriculum
                    cur.execute("INSERT OR IGNORE INTO curricula (id, major_id, attributes, raw) VALUES (?, ?, ?, ?)", 
                                (c_id, m_id, json.dumps(c_attrs, ensure_ascii=False), json.dumps(curr_item, ensure_ascii=False)))
                    total_curricula_saved += 1

                    # Insert Subjects
                    if 'curriculum_curriculum_subjects' in c_attrs and c_attrs['curriculum_curriculum_subjects'].get('data'):
                        for sub_wrapper in c_attrs['curriculum_curriculum_subjects']['data']:
                            if 'curriculum_subject' in sub_wrapper['attributes'] and sub_wrapper['attributes']['curriculum_subject'].get('data'):
                                sub_item = sub_wrapper['attributes']['curriculum_subject']['data']
                                sub_id = sub_item['id']
                                sub_attrs = sub_item['attributes']

                                # Dùng INSERT OR IGNORE để tránh lỗi môn học dùng chung cho nhiều curricula
                                cur.execute("INSERT OR IGNORE INTO subjects (id, curricula_id, attributes, raw) VALUES (?, ?, ?, ?)", 
                                            (sub_id, c_id, json.dumps(sub_attrs, ensure_ascii=False), json.dumps(sub_item, ensure_ascii=False)))
                                total_subjects_saved += 1

        # Kiểm tra điều kiện lặp trang tiếp theo
        meta = data_subjects.get('meta', {}).get('pagination', {})
        page_count = meta.get('pageCount', 1)
        if page >= page_count:
            has_more = False
        else:
            page += 1

    conn.commit()
    conn.close()
    
    print("\n" + "="*50)
    print(" HOÀN TẤT TẠO DATABASE TIẾNG ANH ".center(50, "="))
    print("="*50)
    print(f"✅ Vị trí file: {DB_PATH}")
    print(f"✅ Số lượng Curricula quét được: {total_curricula_saved}")
    print(f"✅ Số lượng Subjects quét được: {total_subjects_saved}")

if __name__ == "__main__":
    fetch_and_save()