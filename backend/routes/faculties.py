from fastapi import APIRouter, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import create_item, delete_item
from backend.routes.common import delete_item_or_404, list_items, update_item_or_404

router = APIRouter(prefix="/faculties", tags=["faculties"])

@router.get("", response_model=PaginatedResponse)
def get_faculties(
    id: Optional[int] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(10, ge=1, le=100),
    school_id: Optional[int] = None,
    search: Optional[str] = Query(None),
):
    return list_items(
        "faculties",
        "Faculty not found",
        id=id,
        page=page,
        page_size=pageSize,
        filters={"school_id": school_id} if school_id is not None else None,
        search=search,
    )

@router.post("")
def create_faculty(item: ItemCreate, school_id: Optional[int] = None):
    id = create_item("faculties", item.attributes, "school_id", school_id)
    return {"id": id, "message": "Faculty created"}

@router.put("/{id}")
def update_faculty(id: int, item: ItemUpdate):
    update_item_or_404("faculties", id, item.attributes, "Faculty not found")
    return {"message": "Faculty updated"}

@router.delete("/{id}")
def delete_faculty(id: int):
    delete_item_or_404("faculties", id, "Faculty not found")
    return {"message": "Faculty deleted"}

@router.post("/bulk-delete")
def bulk_delete_faculties(ids: List[int]):
    deleted = delete_item("faculties", ids=ids)
    return {"message": f"{deleted} faculties deleted"}