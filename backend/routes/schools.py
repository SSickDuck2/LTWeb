from fastapi import APIRouter, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import create_item, delete_item
from backend.routes.common import delete_item_or_404, list_items, update_item_or_404

router = APIRouter(prefix="/schools", tags=["schools"])

@router.get("", response_model=PaginatedResponse)
def get_schools(
    id: Optional[int] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(10, ge=1, le=100),
    search: Optional[str] = Query(None),
):
    return list_items(
        "schools",
        "School not found",
        id=id,
        page=page,
        page_size=pageSize,
        search=search,
    )

@router.post("")
def create_school(item: ItemCreate):
    id = create_item("schools", item.attributes)
    return {"id": id, "message": "School created"}

@router.put("/{id}")
def update_school(id: int, item: ItemUpdate):
    update_item_or_404("schools", id, item.attributes, "School not found")
    return {"message": "School updated"}

@router.delete("/{id}")
def delete_school(id: int):
    delete_item_or_404("schools", id, "School not found")
    return {"message": "School deleted"}

@router.post("/bulk-delete")
def bulk_delete_schools(ids: List[int]):
    deleted = delete_item("schools", ids=ids)
    return {"message": f"{deleted} schools deleted"}