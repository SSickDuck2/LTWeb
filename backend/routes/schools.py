from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import get_table_data, create_item, update_item, delete_item, get_single_item

router = APIRouter(prefix="/schools", tags=["schools"])

@router.get("", response_model=PaginatedResponse)
def get_schools(id: Optional[int] = None, page: int = Query(1, ge=1), pageSize: int = Query(10, ge=1, le=100)):
    if id is not None:
        item = get_single_item("schools", id)
        if not item:
            raise HTTPException(status_code=404, detail="School not found")
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0
        }
    return get_table_data("schools", page=page, page_size=pageSize)

@router.post("")
def create_school(item: ItemCreate):
    id = create_item("schools", item.attributes)
    return {"id": id, "message": "School created"}

@router.put("/{id}")
def update_school(id: int, item: ItemUpdate):
    if not update_item("schools", id, item.attributes):
        raise HTTPException(status_code=404, detail="School not found")
    return {"message": "School updated"}

@router.delete("/{id}")
def delete_school(id: int):
    if delete_item("schools", id=id) == 0:
        raise HTTPException(status_code=404, detail="School not found")
    return {"message": "School deleted"}

@router.post("/bulk-delete")
def bulk_delete_schools(ids: List[int]):
    deleted = delete_item("schools", ids=ids)
    return {"message": f"{deleted} schools deleted"}