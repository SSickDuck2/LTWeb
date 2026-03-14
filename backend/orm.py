from __future__ import annotations

from typing import Optional

from sqlalchemy import ForeignKey, Index, Integer, Text, create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

DEFAULT_DB_PATH = "database/syllabus.db"


def _build_sqlite_url(db_path: str) -> str:
    normalized = db_path.replace("\\", "/")
    return f"sqlite:///{normalized}"


def create_engine_for_path(db_path: str) -> Engine:
    runtime_engine = create_engine(_build_sqlite_url(db_path), future=True)

    @event.listens_for(runtime_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return runtime_engine


engine = create_engine_for_path(DEFAULT_DB_PATH)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    pass


class School(Base):
    __tablename__ = "schools"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    attributes: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[str]] = mapped_column(Text)

    faculties: Mapped[list["Faculty"]] = relationship(back_populates="school")


class Faculty(Base):
    __tablename__ = "faculties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    school_id: Mapped[Optional[int]] = mapped_column(ForeignKey("schools.id", ondelete="SET NULL"), index=True)
    attributes: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[str]] = mapped_column(Text)

    school: Mapped[Optional[School]] = relationship(back_populates="faculties")
    majors: Mapped[list["Major"]] = relationship(back_populates="faculty")


class Major(Base):
    __tablename__ = "majors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    faculty_id: Mapped[Optional[int]] = mapped_column(ForeignKey("faculties.id", ondelete="SET NULL"), index=True)
    attributes: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[str]] = mapped_column(Text)

    faculty: Mapped[Optional[Faculty]] = relationship(back_populates="majors")
    curricula: Mapped[list["Curriculum"]] = relationship(back_populates="major")


class Curriculum(Base):
    __tablename__ = "curricula"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    major_id: Mapped[Optional[int]] = mapped_column(ForeignKey("majors.id", ondelete="SET NULL"), index=True)
    attributes: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[str]] = mapped_column(Text)

    major: Mapped[Optional[Major]] = relationship(back_populates="curricula")
    subject_links: Mapped[list["CurriculumSubject"]] = relationship(back_populates="curriculum")


class Subject(Base):
    __tablename__ = "subjects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    attributes: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[str]] = mapped_column(Text)

    curriculum_links: Mapped[list["CurriculumSubject"]] = relationship(back_populates="subject")


class CurriculumSubject(Base):
    __tablename__ = "curriculum_subjects"
    __table_args__ = (
        Index("idx_curriculum_subjects_curricula_id", "curricula_id"),
        Index("idx_curriculum_subjects_subject_id", "subject_id"),
    )

    curricula_id: Mapped[int] = mapped_column(ForeignKey("curricula.id", ondelete="CASCADE"), primary_key=True)
    subject_id: Mapped[int] = mapped_column(ForeignKey("subjects.id", ondelete="CASCADE"), primary_key=True)
    link_attributes: Mapped[Optional[str]] = mapped_column(Text)

    curriculum: Mapped[Curriculum] = relationship(back_populates="subject_links")
    subject: Mapped[Subject] = relationship(back_populates="curriculum_links")


def create_schema(target_engine: Optional[Engine] = None) -> None:
    runtime_engine = target_engine or engine
    Base.metadata.create_all(bind=runtime_engine)

    with runtime_engine.begin() as conn:
        table_info = conn.exec_driver_sql("PRAGMA table_info(curriculum_subjects)").fetchall()
        column_names = {row[1] for row in table_info}
        if table_info and "link_attributes" not in column_names:
            conn.exec_driver_sql("ALTER TABLE curriculum_subjects ADD COLUMN link_attributes TEXT")


def create_session_factory(db_path: str):
    runtime_engine = create_engine_for_path(db_path)
    create_schema(runtime_engine)
    return runtime_engine, sessionmaker(bind=runtime_engine, autoflush=False, autocommit=False, future=True)
