from typing import TypeVar, Generic
from pydantic import BaseModel

# TODO: add redis

T = TypeVar("T", bound=BaseModel)


class SelectQuery(Generic[T]):
    def __init__(self, model: BaseModel, sql: str, *args):
        self.sql = sql
        self.args = args
        self.model = model


class ExecutableQuery:
    def __init__(self, sql: str, *args):
        self.sql = sql
        self.args = args
