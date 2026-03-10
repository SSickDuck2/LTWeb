from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import get_table_data, create_item, update_item, delete_item, get_single_item

router = APIRouter(prefix="/curricula", tags=["curricula"])

@router.get("", response_model=PaginatedResponse)
def get_curricula(id: Optional[int] = None, page: int = Query(1, ge=1), pageSize: int = Query(10, ge=1, le=100), major_id: Optional[int] = None):
    if id is not None:
        item = get_single_item("curricula", id)
        if not item:
            raise HTTPException(status_code=404, detail="Curriculum not found")
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0
        }
    filters = {"major_id": major_id} if major_id else None
    return get_table_data("curricula", page=page, page_size=pageSize, filters=filters)

@router.post("")
def create_curriculum(item: ItemCreate, major_id: Optional[int] = None):
    id = create_item("curricula", item.attributes, "major_id", major_id)
    return {"id": id, "message": "Curriculum created"}

@router.put("/{id}")
def update_curriculum(id: int, item: ItemUpdate):
    if not update_item("curricula", id, item.attributes):
        raise HTTPException(status_code=404, detail="Curriculum not found")
    return {"message": "Curriculum updated"}

@router.delete("/{id}")
def delete_curriculum(id: int):
    if delete_item("curricula", id=id) == 0:
        raise HTTPException(status_code=404, detail="Curriculum not found")
    return {"message": "Curriculum deleted"}

@router.post("/bulk-delete")
def bulk_delete_curricula(ids: List[int]):
    deleted = delete_item("curricula", ids=ids)
    return {"message": f"{deleted} curricula deleted"}