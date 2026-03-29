from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class ItemCreate(BaseModel):
    attributes: Dict[str, Any]
    attribute_en: Optional[Dict[str, Any]] = None

class ItemUpdate(BaseModel):
    attributes: Optional[Dict[str, Any]] = None
    attribute_en: Optional[Dict[str, Any]] = None

class ItemResponse(BaseModel):
    id: int
    attributes: Optional[Dict[str, Any]]
    raw: Optional[Dict[str, Any]]
    attribute_en: Optional[Dict[str, Any]]
    raw_en: Optional[Dict[str, Any]]
    
class PaginatedResponse(BaseModel):
    data: List[ItemResponse]
    totalRecords: int
    page: int
    pageSize: int
    skippedRecords: int