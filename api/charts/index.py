from typing import Optional, List, Literal

from fastapi import APIRouter, Request, HTTPException, status as fstatus, Query
from core import ChartFastAPI

from database import charts

from helpers.session import get_session, Session

router = APIRouter()


@router.get("/")
async def main(
    request: Request,
    type: Literal["random", "quick", "advanced"] = Query("random"),
    page: int = Query(0, ge=0),
    min_rating: Optional[int] = Query(None),
    max_rating: Optional[int] = Query(None),
    tags: Optional[List[str]] = Query(None),
    min_likes: Optional[int] = Query(None),
    max_likes: Optional[int] = Query(None),
    liked_by: Optional[bool] = Query(False),
    title_includes: Optional[str] = Query(None),
    description_includes: Optional[str] = Query(None),
    artists_includes: Optional[str] = Query(None),
    sort_by: Literal["created_at", "rating", "likes", "decaying_likes", "abc"] = Query(
        "created_at"
    ),
    sort_order: Literal["desc", "asc"] = Query("desc"),
    status: Literal["PUBLIC", "PUBLIC_MINE", "UNLISTED", "PRIVATE", "ALL"] = Query(
        "PUBLIC"
    ),
    meta_includes: Optional[str] = Query(None),
    session: Session = get_session(enforce_auth=False, allow_banned_users=False),
):
    app: ChartFastAPI = request.app

    sonolus_id = session.sonolus_id

    use_owned_by = False
    if status == "ALL":
        status = None
    if status != "PUBLIC":
        if status == None:
            if sonolus_id:
                use_owned_by = True
            else:
                raise HTTPException(
                    status_code=fstatus.HTTP_400_BAD_REQUEST,
                    detail="Not logged in, cannot fetch no status (private level list)",
                )
        elif status in ["UNLISTED", "PRIVATE", "PUBLIC_MINE"]:
            if sonolus_id:
                use_owned_by = True
            else:
                raise HTTPException(
                    status_code=fstatus.HTTP_400_BAD_REQUEST,
                    detail="Not logged in, cannot fetch personal UNLISTED/PRIVATE/PUBLIC_MINE",
                )
    if status == "PUBLIC_MINE":
        status = "PUBLIC"
        use_owned_by = True
    item_page_count = 10
    if type == "random":
        if use_owned_by:
            raise HTTPException(
                status_code=fstatus.HTTP_400_BAD_REQUEST,
                detail="Can't use random for non-public charts.",
            )
        query = charts.get_random_charts(item_page_count // 2, sonolus_id=sonolus_id)
        async with app.db_acquire() as conn:
            rows = await conn.fetch(query)
        # it does convert almost-dict to model to dict, but that adds a layer of "security"
        data = [row.model_dump() for row in rows] if rows else []
        return {"data": data, "asset_base_url": app.s3_asset_base_url}
    if type == "quick":
        if sort_by == "abc":
            sort_order = "asc" if sort_order == "desc" else "desc"
        count_query, quick_list_query = charts.get_chart_list(
            page=page,
            items_per_page=item_page_count,
            meta_includes=meta_includes,
            sort_by=sort_by,
            sort_order=sort_order,
        )
    else:
        if sort_by == "abc":
            sort_order = "asc" if sort_order == "desc" else "desc"
        count_query, chart_list_query = charts.get_chart_list(
            page=page,
            items_per_page=item_page_count,
            min_rating=min_rating,
            max_rating=max_rating,
            status=status,
            tags=tags,
            min_likes=min_likes,
            max_likes=max_likes,
            liked_by=sonolus_id if liked_by else None,
            title_includes=title_includes,
            description_includes=description_includes,
            artists_includes=artists_includes,
            sort_by=sort_by,
            sort_order=sort_order,
            meta_includes=meta_includes,
            sonolus_id=sonolus_id,
            owned_by=sonolus_id if use_owned_by else None,
        )

    async with app.db_acquire() as conn:
        count = (await conn.fetchrow(count_query)).model_dump()
        if count["total_count"] == 0:
            data = []
            page_count = 0
        elif page * item_page_count >= count["total_count"]:
            data = []
            page_count = (count["total_count"] + item_page_count - 1) // item_page_count
        else:
            rows = await conn.fetch(
                quick_list_query if type == "quick" else chart_list_query
            )
            data = [row.model_dump() for row in rows]
            page_count = (count["total_count"] + item_page_count - 1) // item_page_count

    return {
        "pageCount": page_count,
        "data": data,
        "asset_base_url": app.s3_asset_base_url,
    }
