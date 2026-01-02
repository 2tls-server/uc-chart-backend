from typing import Optional, Tuple

from database.query import SelectQuery
from helpers.models import Comment, CommentID, Count, ChartCommentTrend


def create_comment(
    sonolus_id: str, commenter_username: str, chart_id: str, content: str
) -> SelectQuery[CommentID]:
    return SelectQuery(
        CommentID,
        """
            WITH chart_exists AS (
                SELECT 1 FROM charts WHERE id = $3 LIMIT 1
            )
            INSERT INTO comments (commenter, content, created_at, chart_id)
            SELECT $1, $2, CURRENT_TIMESTAMP, $3
            FROM chart_exists
            WHERE EXISTS (SELECT 1 FROM chart_exists)
            RETURNING id;
        """,
        sonolus_id,
        content,
        chart_id,
    )


def delete_comment(
    comment_id: int, sonolus_id: Optional[str] = None
) -> SelectQuery[Comment]:
    if sonolus_id:
        return SelectQuery(
            Comment,
            """
                UPDATE comments
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND commenter = $2 AND deleted_at IS NULL
                RETURNING *;
            """,
            comment_id,
            sonolus_id,
        )
    return SelectQuery(
        Comment,
        """
            UPDATE comments
            SET deleted_at = CURRENT_TIMESTAMP
            WHERE id = $1 AND deleted_at IS NULL
            RETURNING *;
        """,
        comment_id,
    )


def get_comments(
    chart_id: str,
    sonolus_id: Optional[str] = None,
    limit: int = 10,
    page: int = 0,
    sort_desc: bool = True,
    hide_deleted: bool = False,
) -> Tuple[SelectQuery[Comment], SelectQuery[Count]]:
    """
    Returns (comments_query, count_query).
    Use count_query to calculate total pages.
    """
    order_clause = (
        "ORDER BY c.created_at DESC" if sort_desc else "ORDER BY c.created_at ASC"
    )
    offset = page * limit
    comments_query = SelectQuery(
        Comment,
        f"""
            SELECT 
                c.id, 
                c.commenter, 
                CONCAT(a.sonolus_username, '#', a.sonolus_handle) AS username,
                c.content, 
                c.created_at, 
                c.deleted_at, 
                c.chart_id,
                COALESCE(c.commenter = $4, FALSE) AS owner
            FROM comments c
            JOIN accounts a ON c.commenter = a.sonolus_id
            WHERE c.chart_id = $1{' AND c.deleted_at IS NULL' if hide_deleted else ''}
            {order_clause}
            LIMIT $2 OFFSET $3;
        """,
        chart_id,
        limit,
        offset,
        sonolus_id,
    )
    count_query = SelectQuery(
        Count,
        f"""
            SELECT COUNT(*) AS total_count
            FROM comments c
            WHERE c.chart_id = $1{' AND c.deleted_at IS NULL' if hide_deleted else ''};
        """,
        chart_id,
    )
    return comments_query, count_query


def get_comments_by_account(
    sonolus_id: str, limit: int = 3, page: int = 0, sort_desc: bool = False
) -> SelectQuery[Comment]:
    """
    Returns comments by a specific account, including username.
    """
    order_clause = (
        "ORDER BY c.created_at DESC" if sort_desc else "ORDER BY c.created_at ASC"
    )
    offset = page * limit
    return SelectQuery(
        Comment,
        f"""
            SELECT 
                c.id, 
                c.commenter,
                CONCAT(a.sonolus_username, '#', a.sonolus_handle) AS username,
                c.content, 
                c.created_at, 
                c.deleted_at, 
                c.chart_id
            FROM comments c
            JOIN accounts a ON c.commenter = a.sonolus_id
            WHERE c.commenter = $1 AND c.deleted_at IS NULL
            {order_clause}
            LIMIT $2 OFFSET $3;
        """,
        sonolus_id,
        limit,
        offset,
    )


# trend
def fetch_chart_comment_trend(chart_id: str) -> SelectQuery[ChartCommentTrend]:
    return SelectQuery(
        ChartCommentTrend,
        """
        WITH days AS (
            SELECT
                generate_series(
                    CURRENT_DATE - INTERVAL '6 days',
                    CURRENT_DATE,
                    INTERVAL '1 day'
                )::date AS day
        )
        SELECT
            d.day,
            COUNT(c.id) AS total_comments
        FROM days d
        LEFT JOIN charts ch
            ON ch.id = $1
            AND ch.status <> 'PRIVATE'
        LEFT JOIN comments c
            ON c.chart_id = ch.id
            AND c.created_at::date <= d.day
        GROUP BY d.day
        ORDER BY d.day ASC;
        """,
        chart_id,
    )