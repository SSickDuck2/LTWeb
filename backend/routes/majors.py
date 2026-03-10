from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import get_table_data, create_item, update_item, delete_item, get_single_item

router = APIRouter(prefix="/majors", tags=["majors"])

@router.get("", response_model=PaginatedResponse)
def get_majors(id: Optional[int] = None, page: int = Query(1, ge=1), pageSize: int = Query(10, ge=1, le=100), faculty_id: Optional[int] = None):
    if id is not None:
        item = get_single_item("majors", id)
        if not item:
            raise HTTPException(status_code=404, detail="Major not found")
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0
        }
    filters = {"faculty_id": faculty_id} if faculty_id else None
    return get_table_data("majors", page=page, page_size=pageSize, filters=filters)

@router.post("")
def create_major(item: ItemCreate, faculty_id: Optional[int] = None):
    id = create_item("majors", item.attributes, "faculty_id", faculty_id)
    return {"id": id, "message": "Major created"}

@router.put("/{id}")
def update_major(id: int, item: ItemUpdate):
    if not update_item("majors", id, item.attributes):
        raise HTTPException(status_code=404, detail="Major not found")
    return {"message": "Major updated"}

@router.delete("/{id}")
def delete_major(id: int):
    if delete_item("majors", id=id) == 0:
        raise HTTPException(status_code=404, detail="Major not found")
    return {"message": "Major deleted"}

@router.post("/bulk-delete")
def bulk_delete_majors(ids: List[int]):
    deleted = delete_item("majors", ids=ids)
    return {"message": f"{deleted} majors deleted"}