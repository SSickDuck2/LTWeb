from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class ItemCreate(BaseModel):
    attributes: Dict[str, Any]

class ItemUpdate(BaseModel):
    attributes: Optional[Dict[str, Any]] = None

class ItemResponse(BaseModel):
    id: int
    attributes: Dict[str, Any]
    raw: Dict[str, Any]

class PaginatedResponse(BaseModel):
    data: List[ItemResponse]
    totalRecords: int
    page: int
    pageSize: int
    skippedRecords: int