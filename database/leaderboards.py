from typing import Optional, Tuple
from database.query import ExecutableQuery, SelectQuery
from helpers.models import LeaderboardDBResponse, Count, Leaderboard, Prefix


def insert_leaderboard_entry(leaderboard: Leaderboard) -> ExecutableQuery:
    return ExecutableQuery(
        """
        INSERT INTO leaderboards (submitter, replay_data_hash, replay_config_hash, chart_id, engine, nperfect, ngreat, ngood, nmiss, arcade_score, accuracy_score, speed)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """,
        leaderboard.submitter,
        leaderboard.replay_data_hash,
        leaderboard.replay_config_hash,
        leaderboard.chart_id,
        leaderboard.engine,
        leaderboard.nperfect,
        leaderboard.ngreat,
        leaderboard.ngood,
        leaderboard.nmiss,
        leaderboard.arcade_score,
        leaderboard.accuracy_score,
        leaderboard.speed
    )


def get_leaderboard_for_chart(
    chart_id: str,
    limit: int = 10,
    page: int = 0,
    sort_desc: bool = True,
    sonolus_id: Optional[str] = None,
) -> Tuple[SelectQuery[LeaderboardDBResponse], SelectQuery[Count]]:
    """
    Returns (leaderboard_entries_query, count_query).
    Use count_query to calculate total pages.
    """
    order_clause = (
        "ORDER BY l.created_at DESC" if sort_desc else "ORDER BY l.created_at ASC"
    )
    offset = page * limit

    leaderboard_query = SelectQuery(
        LeaderboardDBResponse,
        f"""
            SELECT 
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                COALESCE(l.submitter = $4, FALSE) as owner
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.chart_id = $1
            {order_clause}
            LIMIT $2 OFFSET $3;
        """,
        chart_id,
        limit,
        offset,
        sonolus_id
    )

    count_query = SelectQuery(
        Count,
        """
            SELECT COUNT(*) AS total_count
            FROM leaderboards l
            WHERE l.chart_id = $1;
        """,
        chart_id,
    )

    return (
        leaderboard_query,
        count_query,
    )

def get_leaderboard_prefix_for_user(sonolus_id: str) -> SelectQuery[Prefix]:
    return SelectQuery(
        Prefix,
        """
            SELECT CONCAT(c.chart_author, '/', c.id) AS prefix
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.submitter = $1;
        """,
        sonolus_id
    )

def get_user_leaderboard_for_chart(chart_id: str, sonolus_id: str) -> SelectQuery[LeaderboardDBResponse]:
    return SelectQuery(
        LeaderboardDBResponse,
        """
            SELECT 
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                COALESCE(l.submitter = $4, FALSE) as owner
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.chart_id = $1 AND l.submitter = $2;
        """,
        chart_id,
        sonolus_id
    )

def delete_leaderboard_entry(entry_id: int) -> ExecutableQuery:
    return ExecutableQuery(
        """
        DELETE FROM leaderboards
        WHERE id = $1
        """,
        entry_id,
    )


def delete_leaderboard_for_chart(chart_id: str) -> ExecutableQuery:
    return ExecutableQuery(
        """
        DELETE FROM leaderboards
        WHERE chart_id = $1
        """,
        chart_id,
    )
