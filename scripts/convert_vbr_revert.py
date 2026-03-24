"""
Revert VBR->CBR migration: restore original hashes in DB and delete new S3 objects.

Reads scripts/vbr_changes.json produced by convert_vbr_to_cbr.py.

Usage: python scripts/convert_vbr_revert.py
  Run from the project root (needs config.yml).
"""

import json
import yaml
import asyncio
import asyncpg
import aioboto3

CONCURRENCY = 16
ALLOWED_FIELDS = {"music_file_hash", "preview_file_hash"}
CHANGES_FILE = "scripts/vbr_changes.json"

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

with open(CHANGES_FILE, "r") as f:
    changes = json.load(f)

print(f"Loaded {len(changes)} changes to revert.")


async def main():
    psql = config["psql"]
    pool = await asyncpg.create_pool(
        host=psql["host"],
        user=psql["user"],
        database=psql["database"],
        password=psql["password"],
        port=psql["port"],
        ssl="disable",
    )

    s3_config = config["s3"]
    bucket_name = s3_config["bucket-name"]

    session = aioboto3.Session(
        aws_access_key_id=s3_config["access-key-id"],
        aws_secret_access_key=s3_config["secret-access-key"],
        region_name=s3_config.get("location", "auto"),
    )

    semaphore = asyncio.Semaphore(CONCURRENCY)
    reverted = 0
    errors = 0

    async def revert_one(change, bucket):
        nonlocal reverted, errors
        async with semaphore:
            chart_id = change["chart_id"]
            author = change["author"]
            field = change["field"]
            old_hash = change["old_hash"]
            new_hash = change["new_hash"]

            try:
                if field not in ALLOWED_FIELDS:
                    raise ValueError(f"Invalid field: {field}")
                async with pool.acquire() as conn:
                    await conn.execute(
                        f"UPDATE charts SET {field} = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                        old_hash,
                        chart_id,
                    )

                # delete the new CBR S3 object
                if new_hash != old_hash:
                    new_key = f"{author}/{chart_id}/{new_hash}"
                    obj = await bucket.Object(new_key)
                    await obj.delete()

                reverted += 1
                print(f"  [{chart_id}] {field}: reverted {new_hash} -> {old_hash}")
            except Exception as e:
                errors += 1
                print(f"  [{chart_id}] {field} ERROR: {e}")

    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(bucket_name)
        tasks = [revert_one(change, bucket) for change in changes]
        await asyncio.gather(*tasks)

    print(f"\nDone! Reverted: {reverted}, Errors: {errors}")

    await pool.close()


asyncio.run(main())
