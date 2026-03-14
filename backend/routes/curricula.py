from fastapi import APIRouter, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import create_item, delete_item
from backend.routes.common import delete_item_or_404, list_items, update_item_or_404

router = APIRouter(prefix="/curricula", tags=["curricula"])

@router.get("", response_model=PaginatedResponse)
def get_curricula(
    id: Optional[int] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(10, ge=1, le=100),
    major_id: Optional[int] = None,
    search: Optional[str] = Query(None),
):
    return list_items(
        "curricula",
        "Curriculum not found",
        id=id,
        page=page,
        page_size=pageSize,
        filters={"major_id": major_id} if major_id is not None else None,
        search=search,
    )

@router.post("")
def create_curriculum(item: ItemCreate, major_id: Optional[int] = None):
    id = create_item("curricula", item.attributes, "major_id", major_id)
    return {"id": id, "message": "Curriculum created"}

@router.put("/{id}")
def update_curriculum(id: int, item: ItemUpdate):
    update_item_or_404("curricula", id, item.attributes, "Curriculum not found")
    return {"message": "Curriculum updated"}

@router.delete("/{id}")
def delete_curriculum(id: int):
    delete_item_or_404("curricula", id, "Curriculum not found")
    return {"message": "Curriculum deleted"}

@router.post("/bulk-delete")
def bulk_delete_curricula(ids: List[int]):
    deleted = delete_item("curricula", ids=ids)
    return {"message": f"{deleted} curricula deleted"}