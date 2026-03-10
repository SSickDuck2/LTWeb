from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.models import ItemCreate, ItemUpdate, PaginatedResponse
from backend.database import get_table_data, create_item, update_item, delete_item, get_single_item

router = APIRouter(prefix="/faculties", tags=["faculties"])

@router.get("", response_model=PaginatedResponse)
def get_faculties(id: Optional[int] = None, page: int = Query(1, ge=1), pageSize: int = Query(10, ge=1, le=100), school_id: Optional[int] = None):
    if id is not None:
        item = get_single_item("faculties", id)
        if not item:
            raise HTTPException(status_code=404, detail="Faculty not found")
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0
        }
    filters = {"school_id": school_id} if school_id else None
    return get_table_data("faculties", page=page, page_size=pageSize, filters=filters)

@router.post("")
def create_faculty(item: ItemCreate, school_id: Optional[int] = None):
    id = create_item("faculties", item.attributes, "school_id", school_id)
    return {"id": id, "message": "Faculty created"}

@router.put("/{id}")
def update_faculty(id: int, item: ItemUpdate):
    if not update_item("faculties", id, item.attributes):
        raise HTTPException(status_code=404, detail="Faculty not found")
    return {"message": "Faculty updated"}

@router.delete("/{id}")
def delete_faculty(id: int):
    if delete_item("faculties", id=id) == 0:
        raise HTTPException(status_code=404, detail="Faculty not found")
    return {"message": "Faculty deleted"}

@router.post("/bulk-delete")
def bulk_delete_faculties(ids: List[int]):
    deleted = delete_item("faculties", ids=ids)
    return {"message": f"{deleted} faculties deleted"}