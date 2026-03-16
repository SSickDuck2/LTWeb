from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class ItemCreate(BaseModel):
    attribute_vn: Dict[str, Any]
    attribute_en: Optional[Dict[str, Any]] = None

class ItemUpdate(BaseModel):
    attribute_vn: Optional[Dict[str, Any]] = None
    attribute_en: Optional[Dict[str, Any]] = None

class ItemResponse(BaseModel):
    id: int
    attribute_vn: Optional[Dict[str, Any]]
    raw_vn: Optional[Dict[str, Any]]
    attribute_en: Optional[Dict[str, Any]]
    raw_en: Optional[Dict[str, Any]]
    
class PaginatedResponse(BaseModel):
    data: List[ItemResponse]
    totalRecords: int
    page: int
    pageSize: int
    skippedRecords: int