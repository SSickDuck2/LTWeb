from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import get_table_data, create_item, update_item, delete_item, get_single_item

router = APIRouter(prefix="/subjects", tags=["subjects"])

@router.get("", response_model=PaginatedResponse)
def get_subjects(id: Optional[int] = None, page: int = Query(1, ge=1), pageSize: int = Query(10, ge=1, le=100), curricula_id: Optional[int] = None):
    if id is not None:
        item = get_single_item("subjects", id)
        if not item:
            raise HTTPException(status_code=404, detail="Subject not found")
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0
        }
    filters = {"curricula_id": curricula_id} if curricula_id else None
    return get_table_data("subjects", page=page, page_size=pageSize, filters=filters)

@router.post("")
def create_subject(item: ItemCreate, curricula_id: Optional[int] = None):
    id = create_item("subjects", item.attributes, "curricula_id", curricula_id)
    return {"id": id, "message": "Subject created"}

@router.put("/{id}")
def update_subject(id: int, item: ItemUpdate):
    if not update_item("subjects", id, item.attributes):
        raise HTTPException(status_code=404, detail="Subject not found")
    return {"message": "Subject updated"}

@router.delete("/{id}")
def delete_subject(id: int):
    if delete_item("subjects", id=id) == 0:
        raise HTTPException(status_code=404, detail="Subject not found")
    return {"message": "Subject deleted"}

@router.post("/bulk-delete")
def bulk_delete_subjects(ids: List[int]):
    deleted = delete_item("subjects", ids=ids)
    return {"message": f"{deleted} subjects deleted"}