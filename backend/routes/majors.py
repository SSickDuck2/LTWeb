from fastapi import APIRouter, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import create_item, delete_item
from backend.routes.common import delete_item_or_404, list_items, update_item_or_404

router = APIRouter(prefix="/majors", tags=["majors"])

@router.get("", response_model=PaginatedResponse)
def get_majors(
    id: Optional[int] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(10, ge=1, le=100),
    faculty_id: Optional[int] = None,
    search: Optional[str] = Query(None),
):
    return list_items(
        "majors",
        "Major not found",
        id=id,
        page=page,
        page_size=pageSize,
        filters={"faculty_id": faculty_id} if faculty_id is not None else None,
        search=search,
    )

@router.post("")
def create_major(item: ItemCreate, faculty_id: Optional[int] = None):
    id = create_item("majors", item.attributes, "faculty_id", faculty_id)
    return {"id": id, "message": "Major created"}

@router.put("/{id}")
def update_major(id: int, item: ItemUpdate):
    update_item_or_404("majors", id, item.attributes, "Major not found")
    return {"message": "Major updated"}

@router.delete("/{id}")
def delete_major(id: int):
    delete_item_or_404("majors", id, "Major not found")
    return {"message": "Major deleted"}

@router.post("/bulk-delete")
def bulk_delete_majors(ids: List[int]):
    deleted = delete_item("majors", ids=ids)
    return {"message": f"{deleted} majors deleted"}