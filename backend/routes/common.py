from typing import Any, Dict, Optional

from fastapi import HTTPException

from backend.database import delete_item, get_single_item, get_table_data, update_item


def list_items(
    table: str,
    not_found_message: str,
    id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
    filters: Optional[Dict[str, Any]] = None,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    if id is not None:
        item = get_single_item(table, id)
        if not item:
            raise HTTPException(status_code=404, detail=not_found_message)
        return {
            "data": [item],
            "totalRecords": 1,
            "page": 1,
            "pageSize": 1,
            "skippedRecords": 0,
        }

    return get_table_data(
        table,
        page=page,
        page_size=page_size,
        filters=filters,
        search=search,
    )


def update_item_or_404(
    table: str,
    id: int,
    attributes: Optional[Dict[str, Any]],
    not_found_message: str,
) -> None:
    if not update_item(table, id, attributes):
        raise HTTPException(status_code=404, detail=not_found_message)


def delete_item_or_404(table: str, id: int, not_found_message: str) -> None:
    if delete_item(table, id=id) == 0:
        raise HTTPException(status_code=404, detail=not_found_message)
