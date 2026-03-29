from __future__ import annotations

import os
from typing import Optional
from urllib.parse import quote_plus

from sqlalchemy import JSON, ForeignKey, Index, Integer, BigInteger, Numeric, create_engine, Column, String, DateTime, Text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.sql import func


def _load_env_file(env_path: str = ".env") -> None:
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ[key] = value


def _first_non_empty_env(*keys: str) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return None


def _parse_conninfo_kv(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in text.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip().lower()
        value = value.strip().strip('"').strip("'")
        if key and value:
            result[key] = value
    return result


def _build_postgres_url_from_parts(seed: Optional[dict[str, str]] = None) -> Optional[str]:
    parts = dict(seed or {})

    parts.setdefault("user", _first_non_empty_env("SUPABASE_DB_USER", "SUPABASE_USER", "PGUSER", "user") or "")
    parts.setdefault(
        "password",
        _first_non_empty_env("SUPABASE_DB_PASSWORD", "SUPABASE_PASSWORD", "PGPASSWORD", "password") or "",
    )
    parts.setdefault("host", _first_non_empty_env("SUPABASE_DB_HOST", "SUPABASE_HOST", "PGHOST", "host") or "")
    parts.setdefault("port", _first_non_empty_env("SUPABASE_DB_PORT", "SUPABASE_PORT", "PGPORT", "port") or "5432")
    parts.setdefault(
        "dbname",
        _first_non_empty_env("SUPABASE_DB_NAME", "SUPABASE_DB_DBNAME", "PGDATABASE", "dbname") or "postgres",
    )
    sslmode = _first_non_empty_env("SUPABASE_DB_SSLMODE", "PGSSLMODE", "sslmode")
    if sslmode and "sslmode" not in parts:
        parts["sslmode"] = sslmode

    user = parts.get("user")
    host = parts.get("host")
    if not user or not host:
        return None

    password = parts.get("password") or ""
    port = parts.get("port") or "5432"
    dbname = parts.get("dbname") or "postgres"
    sslmode = parts.get("sslmode")

    auth = quote_plus(user)
    if password:
        auth += f":{quote_plus(password)}"

    url = f"postgresql+psycopg://{auth}@{host}:{port}/{quote_plus(dbname)}"
    if sslmode:
        url += f"?sslmode={quote_plus(sslmode)}"

    return url


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def resolve_runtime_database_url() -> str:
    _load_env_file()

    direct = _first_non_empty_env("DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_URL", "SUPABSE_URL")
    if direct and "://" in direct:
        normalized = _normalize_database_url(direct)
        if normalized.startswith("postgresql://"):
            return normalized.replace("postgresql://", "postgresql+psycopg://", 1)
        return normalized

    seed = _parse_conninfo_kv(direct) if direct and "=" in direct else None
    postgres_url = _build_postgres_url_from_parts(seed)
    if postgres_url:
        return postgres_url

    raise RuntimeError(
        "Missing Supabase/PostgreSQL connection. Set DATABASE_URL or SUPABASE_DB_URL in .env."
    )


def create_engine_for_url(database_url: str) -> Engine:
    return create_engine(database_url, future=True, pool_pre_ping=True)


engine = create_engine_for_url(resolve_runtime_database_url())
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    pass


class School(Base):
    __tablename__ = "schools_new"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    en_name: Mapped[Optional[str]] = mapped_column(Text)
    en_slug: Mapped[Optional[str]] = mapped_column(Text)
    en_locale: Mapped[Optional[str]] = mapped_column(Text)
    en_description: Mapped[Optional[str]] = mapped_column(Text)
    en_code: Mapped[Optional[str]] = mapped_column(Text)
    en_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    vn_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_locale: Mapped[Optional[str]] = mapped_column(Text)
    vn_description: Mapped[Optional[str]] = mapped_column(Text)
    vn_code: Mapped[Optional[str]] = mapped_column(Text)
    vn_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))

    faculties: Mapped[list["Faculty"]] = relationship(back_populates="school")


class Faculty(Base):
    __tablename__ = "faculties_new"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    school_id: Mapped[Optional[int]] = mapped_column(ForeignKey("schools_new.id", ondelete="SET NULL"), index=True)
    en_name: Mapped[Optional[str]] = mapped_column(Text)
    en_slug: Mapped[Optional[str]] = mapped_column(Text)
    en_locale: Mapped[Optional[str]] = mapped_column(Text)
    en_description: Mapped[Optional[str]] = mapped_column(Text)
    en_code: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    en_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    vn_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_locale: Mapped[Optional[str]] = mapped_column(Text)
    vn_description: Mapped[Optional[str]] = mapped_column(Text)
    vn_code: Mapped[Optional[str]] = mapped_column(Text)
    vn_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)

    school: Mapped[Optional[School]] = relationship(back_populates="faculties")
    majors: Mapped[list["Major"]] = relationship(back_populates="faculty")


class Major(Base):
    __tablename__ = "majors_new"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    faculty_id: Mapped[Optional[int]] = mapped_column(ForeignKey("faculties_new.id", ondelete="SET NULL"), index=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    slug: Mapped[Optional[str]] = mapped_column(Text)
    locale: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    faculty_code: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    en_name: Mapped[Optional[str]] = mapped_column(Text)
    en_slug: Mapped[Optional[str]] = mapped_column(Text)
    en_locale: Mapped[Optional[str]] = mapped_column(Text)
    en_description: Mapped[Optional[str]] = mapped_column(Text)
    en_faculty_code: Mapped[Optional[str]] = mapped_column(Text)
    en_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    vn_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_locale: Mapped[Optional[str]] = mapped_column(Text)
    vn_description: Mapped[Optional[str]] = mapped_column(Text)
    vn_faculty_code: Mapped[Optional[str]] = mapped_column(Text)
    vn_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)

    faculty: Mapped[Optional[Faculty]] = relationship(back_populates="majors")
    curricula: Mapped[list["Curriculum"]] = relationship(back_populates="major")


class Curriculum(Base):
    __tablename__ = "curriculum_new"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    major_id: Mapped[Optional[int]] = mapped_column(ForeignKey("majors_new.id", ondelete="SET NULL"), index=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    slug: Mapped[Optional[str]] = mapped_column(Text)
    locale: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    code: Mapped[Optional[str]] = mapped_column(Text)
    credits: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    effective_year: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    en_name: Mapped[Optional[str]] = mapped_column(Text)
    en_slug: Mapped[Optional[str]] = mapped_column(Text)
    en_locale: Mapped[Optional[str]] = mapped_column(Text)
    en_description: Mapped[Optional[str]] = mapped_column(Text)
    en_code: Mapped[Optional[str]] = mapped_column(Text)
    en_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    vn_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_locale: Mapped[Optional[str]] = mapped_column(Text)
    vn_description: Mapped[Optional[str]] = mapped_column(Text)
    vn_code: Mapped[Optional[str]] = mapped_column(Text)
    vn_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)

    major: Mapped[Optional[Major]] = relationship(back_populates="curricula")
    subject_links: Mapped[list["CurriculumSubject"]] = relationship(back_populates="curriculum")


class Subject(Base):
    __tablename__ = "subjects_new"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    code: Mapped[Optional[str]] = mapped_column(Text)
    slug: Mapped[Optional[str]] = mapped_column(Text)
    name: Mapped[Optional[str]] = mapped_column(Text)
    locale: Mapped[Optional[str]] = mapped_column(Text)
    short_name: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    credits: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    lecture_hours: Mapped[Optional[int]] = mapped_column(Integer)
    practice_hours: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    en_name: Mapped[Optional[str]] = mapped_column(Text)
    en_slug: Mapped[Optional[str]] = mapped_column(Text)
    en_locale: Mapped[Optional[str]] = mapped_column(Text)
    en_short_name: Mapped[Optional[str]] = mapped_column(Text)
    en_description: Mapped[Optional[str]] = mapped_column(Text)
    en_code: Mapped[Optional[str]] = mapped_column(Text)
    en_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    vn_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_locale: Mapped[Optional[str]] = mapped_column(Text)
    vn_short_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_description: Mapped[Optional[str]] = mapped_column(Text)
    vn_code: Mapped[Optional[str]] = mapped_column(Text)
    vn_raw_attributes: Mapped[Optional[dict]] = mapped_column(JSON)

    curriculum_links: Mapped[list["CurriculumSubject"]] = relationship(back_populates="subject")

class CurriculumSubject(Base):
    __tablename__ = "curriculum_subjects_new"
    __table_args__ = (
        Index("idx_curriculum_subjects_curricula_id", "curricula_id"),
        Index("idx_curriculum_subjects_subject_id", "subject_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    curricula_id: Mapped[int] = mapped_column(ForeignKey("curriculum_new.id", ondelete="CASCADE"), index=True)
    subject_id: Mapped[int] = mapped_column(ForeignKey("subjects_new.id", ondelete="CASCADE"), index=True)
    semester: Mapped[Optional[int]] = mapped_column(Integer)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    mandatory: Mapped[Optional[bool]] = mapped_column()
    credit_value: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    link_note: Mapped[Optional[str]] = mapped_column(Text)
    link_attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True))
    en_note: Mapped[Optional[str]] = mapped_column(Text)
    en_language: Mapped[Optional[str]] = mapped_column(Text)
    en_curriculum_subject_name: Mapped[Optional[str]] = mapped_column(Text)
    en_curriculum_subject_slug: Mapped[Optional[str]] = mapped_column(Text)
    vn_note: Mapped[Optional[str]] = mapped_column(Text)
    vn_language: Mapped[Optional[str]] = mapped_column(Text)
    vn_curriculum_subject_name: Mapped[Optional[str]] = mapped_column(Text)
    vn_curriculum_subject_slug: Mapped[Optional[str]] = mapped_column(Text)

    curriculum: Mapped[Curriculum] = relationship(back_populates="subject_links")
    subject: Mapped[Subject] = relationship(back_populates="curriculum_links")

class Teacher(Base):
    __tablename__ = "teachers"

    teacher_code = Column(String(20), primary_key=True, index=True)
    full_name = Column(String(100), nullable=False)
    password_hash = Column(String(255), nullable=False)
    
    school_id = Column(Integer, nullable=True)
    faculty_id = Column(Integer, nullable=True)
    major_id = Column(Integer, nullable=True)
    curricula_id = Column(Integer, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

def create_schema(target_engine: Optional[Engine] = None) -> None:
    runtime_engine = target_engine or engine
    Base.metadata.create_all(bind=runtime_engine)
