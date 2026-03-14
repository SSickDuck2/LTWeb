from fastapi import APIRouter, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import create_item, delete_item
from backend.routes.common import delete_item_or_404, list_items, update_item_or_404

router = APIRouter(prefix="/subjects", tags=["subjects"])

@router.get("", response_model=PaginatedResponse)
def get_subjects(
    id: Optional[int] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(10, ge=1, le=100),
    curricula_id: Optional[int] = None,
    search: Optional[str] = Query(None),
):
    return list_items(
        "subjects",
        "Subject not found",
        id=id,
        page=page,
        page_size=pageSize,
        filters={"curricula_id": curricula_id} if curricula_id is not None else None,
        search=search,
    )

@router.post("")
def create_subject(item: ItemCreate, curricula_id: Optional[int] = None):
    id = create_item("subjects", item.attributes, "curricula_id", curricula_id)
    return {"id": id, "message": "Subject created"}

@router.put("/{id}")
def update_subject(id: int, item: ItemUpdate):
    update_item_or_404("subjects", id, item.attributes, "Subject not found")
    return {"message": "Subject updated"}

@router.delete("/{id}")
def delete_subject(id: int):
    delete_item_or_404("subjects", id, "Subject not found")
    return {"message": "Subject deleted"}

@router.post("/bulk-delete")
def bulk_delete_subjects(ids: List[int]):
    deleted = delete_item("subjects", ids=ids)
    return {"message": f"{deleted} subjects deleted"}