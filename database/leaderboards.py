from typing import Literal, Optional, Tuple
from database.query import ExecutableQuery, SelectQuery
from helpers.models import LeaderboardRecordDBResponse, Count, LeaderboardRecord, Prefix


def create_leaderboard_record(record: LeaderboardRecord) -> ExecutableQuery:
    return ExecutableQuery(
        """
        INSERT INTO leaderboards (
            submitter, 
            replay_data_hash, 
            replay_config_hash, 
            chart_id, 
            engine, 
            grade, 
            nperfect, 
            ngreat, 
            ngood, 
            nmiss, 
            arcade_score, 
            accuracy_score, 
            speed, 
            display_name, 
            public_chart
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """,
        record.submitter,
        record.replay_data_hash,
        record.replay_config_hash,
        record.chart_id,
        record.engine,
        record.grade,
        record.nperfect,
        record.ngreat,
        record.ngood,
        record.nmiss,
        record.arcade_score,
        record.accuracy_score,
        record.speed,
        record.display_name,
        record.public_chart
    )


def get_leaderboards_for_chart(
    chart_id: str,
    limit: int = 10,
    page: int = 0,
    sonolus_id: Optional[str] = None,
) -> Tuple[SelectQuery[LeaderboardRecordDBResponse], SelectQuery[Count]]:
    """
    Returns (leaderboard_entries_query, count_query).
    Use count_query to calculate total pages.
    """
    offset = page * limit

    leaderboard_query = SelectQuery(
        LeaderboardRecordDBResponse,
        f"""
            SELECT 
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.replay_config_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.grade,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                l.display_name,
                l.public_chart
                COALESCE(c.submitter = $4, FALSE) AS owner
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.chart_id = $1
            ORDER BY l.arcade_score DESC
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

def get_leaderboard_record_by_id(
    chart_id: str,
    record_id: int,
    sonolus_id: str | None = None
) -> SelectQuery[LeaderboardRecordDBResponse]:
    return SelectQuery(
        LeaderboardRecordDBResponse,
        """
            SELECT 
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.replay_config_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.grade,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                l.display_name,
                l.public_chart
                COALESCE(c.submitter = $4, FALSE) AS owner,
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.chart_id = $1 AND l.id = $2
        """,
        chart_id, record_id, sonolus_id
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

def get_user_leaderboard_record_for_chart(chart_id: str, sonolus_id: str) -> SelectQuery[LeaderboardRecordDBResponse]:
    return SelectQuery(
        LeaderboardRecordDBResponse,
        """
            SELECT 
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.replay_config_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.grade,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                l.display_name,
                l.public_chart
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.chart_id = $1 AND l.submitter = $2;
        """,
        chart_id,
        sonolus_id
    )

def delete_leaderboard_record(record_id: int) -> ExecutableQuery:
    return ExecutableQuery(
        """
        DELETE FROM leaderboards
        WHERE id = $1
        """,
        record_id,
    )

def update_leaderboard_visibility(chart_id: str, status: Literal["PUBLIC", "PRIVATE", "UNLISTED"]) -> ExecutableQuery:
    return ExecutableQuery(
        """
        UPDATE leaderboards
        SET 
            public_chart = $2,
        WHERE chart_id = $1
        """,
        chart_id, status == "PUBLIC"
    )

def get_random_leaderboard_records(limit: int) -> SelectQuery[LeaderboardRecordDBResponse]:
    return SelectQuery(
        LeaderboardRecordDBResponse,
        """
            SELECT
                l.id,
                l.submitter,
                l.replay_data_hash,
                l.replay_config_hash,
                l.chart_id,
                l.created_at,
                CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                l.engine,
                l.grade,
                l.nperfect,
                l.ngreat,
                l.ngood,
                l.nmiss,
                l.arcade_score,
                l.accuracy_score,
                l.speed,
                l.display_name,
                l.public_chart
            FROM leaderboards l
            JOIN charts c ON l.chart_id = c.id
            WHERE l.public_chart
            ORDER BY RANDOM() LIMIT $1;
        """,
        limit
    )

def get_public_records(limit: int, page: int = 0) -> tuple[SelectQuery[LeaderboardRecordDBResponse], SelectQuery[Count]]:
    return (
        SelectQuery(
            LeaderboardRecordDBResponse,
            """
                SELECT
                    l.id,
                    l.submitter,
                    l.replay_data_hash,
                    l.replay_config_hash,
                    l.chart_id,
                    l.created_at,
                    CONCAT(c.chart_author, '/', c.id) AS chart_prefix,
                    l.engine,
                    l.grade,
                    l.nperfect,
                    l.ngreat,
                    l.ngood,
                    l.nmiss,
                    l.arcade_score,
                    l.accuracy_score,
                    l.speed,
                    l.display_name,
                    l.public_chart
                FROM leaderboards l
                JOIN charts c ON l.chart_id = c.id
                WHERE l.public_chart
                LIMIT $1 OFFSET $2;
            """,
            limit,
            page * limit
        ),
        SelectQuery(
            Count,
            """
                SELECT COUNT(*) AS total_count
                FROM leaderboards l
                WHERE l.public_chart;
            """,
        )
    )