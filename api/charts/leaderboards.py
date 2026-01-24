"""
Unlike charts/{id}/leaderboards, returns all public records
"""

import math
from typing import Literal
from fastapi import APIRouter, Request

from core import ChartFastAPI
from database import charts, leaderboards
from helpers.models import ChartByID

router = APIRouter()

@router.get("/random")
async def get(
    request: Request,
    limit: Literal[3, 10] = 10
):
    app: ChartFastAPI = request.app

    async with app.db_acquire() as conn:
        records = await conn.fetch(leaderboards.get_random_leaderboard_records(limit))
        response = {"data": []}

        charts_dict = {
            chart.id: chart
            for chart in 
            await conn.fetch(
                charts.get_chart_by_id_batch(
                    list(set([record.chart_id for record in records]))
                )
            )
        }

        for record in records:
            record_data = {
                "data": record.model_dump(),
                "chart": charts_dict[record.chart_id],
                "asset_base_url": app.s3_asset_base_url
            }

            response["data"].append(record_data)

    return response


@router.get("/")
async def get(
    request: Request,
    limit: Literal[3, 10] = 10,
    page: int = 0
):
    app: ChartFastAPI = request.app

    leaderboard_query, count_query = leaderboards.get_public_records(limit, page)

    async with app.db_acquire() as conn:
        records = await conn.fetch(leaderboard_query)
        response = {"data": []}

        charts_dict = {
            chart.id: chart
            for chart in 
            await conn.fetch(
                charts.get_chart_by_id_batch(
                    list(set([record.chart_id for record in records]))
                )
            )
        }

        for record in records:
            record_data = {
                "data": record.model_dump(),
                "chart": charts_dict[record.chart_id],
                "asset_base_url": app.s3_asset_base_url
            }

            response["data"].append(record_data)

        if limit != 3:
            response["pageCount"] = math.ceil((await conn.fetchrow(count_query)) / 10)

    return response