import asyncio

from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from database import accounts

router = APIRouter()


@router.patch("/ban/")
async def ban_user(request: Request, id: str, delete: bool = False):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_banned(id, True)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        if delete:
            await conn.conn.execute("DELETE FROM charts WHERE author = $1", id)

    if delete:
        bucket_name = app.s3_bucket

        prefixes = []  # XXX: grab from database, should be chart_author_id/chart_id

        async with app.s3_session_getter() as s3:
            bucket = await s3.Bucket(bucket_name)

            delete_batches = []

            # Delete everything under {id}/
            batch = []
            async for obj in bucket.objects.filter(Prefix=f"{id}/"):
                batch.append({"Key": obj.key})
                if len(batch) == 1000:
                    delete_batches.append(batch)
                    batch = []
            if batch:
                delete_batches.append(batch)

            # Delete everything under {prefix}/replays/{id}/
            for prefix in prefixes:
                full_prefix = f"{prefix}/replays/{id}/"
                batch = []
                async for obj in bucket.objects.filter(Prefix=full_prefix):
                    batch.append({"Key": obj.key})
                    if len(batch) == 1000:
                        delete_batches.append(batch)
                        batch = []
                if batch:
                    delete_batches.append(batch)

            tasks = [
                bucket.delete_objects(Delete={"Objects": delete_batch})
                for delete_batch in delete_batches
            ]
            await asyncio.gather(*tasks)
    return {"result": "success"}


@router.patch("/unban/")
async def unban_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_banned(id, False)

    async with app.db_acquire() as conn:
        await conn.execute(query)

    return {"result": "success"}
