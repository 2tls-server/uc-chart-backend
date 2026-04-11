from typing import Optional

from database.query import SelectQuery, ExecutableQuery
from helpers.models import StaffAction


def log_action(
    actor_id: str,
    action: str,
    target_type: str,
    target_id: str,
    previous_value: Optional[str] = None,
    new_value: Optional[str] = None,
) -> ExecutableQuery:
    return ExecutableQuery(
        """
            INSERT INTO staff_actions (actor_id, action, target_type, target_id, previous_value, new_value)
            VALUES ($1, $2, $3, $4, $5, $6);
        """,
        actor_id,
        action,
        target_type,
        target_id,
        previous_value,
        new_value,
    )


def get_actions_since(since_ts: str, limit: int = 100) -> SelectQuery[StaffAction]:
    return SelectQuery(
        StaffAction,
        """
            SELECT sa.*, a.sonolus_username, a.sonolus_handle
            FROM staff_actions sa
            JOIN accounts a ON sa.actor_id = a.sonolus_id
            WHERE sa.created_at >= $1::timestamptz
            ORDER BY sa.created_at DESC
            LIMIT $2;
        """,
        since_ts,
        limit,
    )
