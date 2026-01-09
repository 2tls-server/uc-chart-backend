from fastapi import APIRouter, Request, HTTPException, status
from core import ChartFastAPI

from database import charts, comments
from helpers.session import get_session, Session

from typing import List

router = APIRouter()


def scale_trend(values: List[int]) -> List[int]:
    """
    Scale a list of cumulative totals into integers 1-100.
    Day 1 maps to 1, last day maps to 100 (or less if flat).
    """
    if not values:
        return [1] * 7  # fallback if empty

    min_val = min(values)
    max_val = max(values)

    if max_val == min_val:
        # All values equal → return all 1s
        return [1] * len(values)

    scaled = [
        max(1, int(round(1 + 99 * (v - min_val) / (max_val - min_val)))) for v in values
    ]
    return scaled


@router.get("/")
async def main(request: Request, id: str, session: Session = get_session()):
    # exposed to public
    # no authentication needed
    # however, if they are authed
    # use it to check if liked

    app: ChartFastAPI = request.app

    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )

    query_likes = charts.fetch_chart_like_trend(id)
    query_comments = comments.fetch_chart_comment_trend(id)

    async with app.db_acquire() as conn:
        result = await conn.fetch(query_likes)
        result2 = await conn.fetch(query_comments)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Chart not found."
            )

    likes_totals = [row.total_likes for row in result]
    comments_totals = [row.total_comments for row in result2]

    likes_scaled = scale_trend(likes_totals)
    comments_scaled = scale_trend(comments_totals)

    return {
        "likes": likes_scaled,
        "comments": comments_scaled,
    }
