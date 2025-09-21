import math

from fastapi import APIRouter, Request, HTTPException, status, Query
from typing import Optional

from core import ChartFastAPI

from database import comments
from helpers.session import get_session, Session

from helpers.models import CommentRequest

router = APIRouter()


@router.post("/")
async def main(
    request: Request,
    id: str,
    data: CommentRequest,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    ),
):
    # exposed to public
    # authentication needed

    app: ChartFastAPI = request.app

    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )
    if len(data.content) > 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Comments cannot be larger than 200 characters.",
        )
    user = await session.user()
    query = comments.create_comment(
        user.sonolus_id, user.sonolus_username, id, data.content
    )
    async with app.db_acquire() as conn:
        result = await conn.fetchrow(query)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Chart not found."
            )
    return {"result": "success"}


@router.delete("/{comment_id}/")
async def main(
    request: Request,
    id: str,
    comment_id: int,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    ),
):
    # exposed to public
    # authentication needed

    app: ChartFastAPI = request.app

    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )
    user = await session.user()
    if user.mod:
        query = comments.delete_comment(comment_id)
    else:
        query = comments.delete_comment(comment_id, user.sonolus_id)
    async with app.db_acquire() as conn:
        result = await conn.fetchrow(query)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Chart or comment not found.",
            )
    return {"result": "success"}


@router.get("/")
async def main(
    request: Request,
    id: str,
    page: Optional[int] = Query(0, ge=0),
    session: Session = get_session(
        enforce_auth=False, enforce_type="game", allow_banned_users=False
    ),
):
    app: ChartFastAPI = request.app

    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )
    user = None
    if session.auth:
        user = await session.user()
    query, count_query = comments.get_comments(
        id, sonolus_id=user.sonolus_id if user else None, page=page
    )

    async with app.db_acquire() as conn:
        count_result = await conn.fetchrow(count_query)
        total_count = count_result.total_count if count_result else 0
        page_count = math.ceil(total_count / 10) if total_count > 0 else 0

        if page_count == 0 or page >= page_count:
            return {"data": [], "pageCount": page_count}

        result = await conn.fetch(query)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Chart not found."
            )
    data = [
        {
            **row.model_dump(),
            "created_at": int(row.created_at.timestamp() * 1000),
            "deleted_at": (
                int(row.deleted_at.timestamp() * 1000) if row.deleted_at else None
            ),
        }
        for row in result
    ]
    for comment in data:
        if comment["deleted_at"]:
            comment["content"] = (
                "[DELETED]"
                if (user and not user.mod)
                else f"[DELETED]\nMod View:\n{'-'*10}\n{comment['content']}"
            )
    ret = {"data": data, "pageCount": page_count}
    if user and user.mod:
        ret["mod"] = True
        if user.admin:
            ret["admin"] = True
    return ret
