"""
Commit VBR->CBR migration: delete old VBR S3 objects that are no longer referenced.

Reads scripts/vbr_changes.json produced by convert_vbr_to_cbr.py.

Usage: python scripts/convert_vbr_commit.py
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

print(f"Loaded {len(changes)} changes to commit.")


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

    print("Verifying DB state...")
    safe_to_delete = []
    skipped = 0

    async with pool.acquire() as conn:
        for change in changes:
            chart_id = change["chart_id"]
            field = change["field"]
            new_hash = change["new_hash"]
            old_hash = change["old_hash"]

            if field not in ALLOWED_FIELDS:
                print(f"  [{chart_id}] invalid field '{field}', skipping")
                skipped += 1
                continue

            if new_hash == old_hash:
                skipped += 1
                continue

            row = await conn.fetchrow(
                f"SELECT {field} FROM charts WHERE id = $1",
                chart_id,
            )
            if not row:
                print(f"  [{chart_id}] chart not found, skipping")
                skipped += 1
                continue

            current_hash = row[field]
            if current_hash != new_hash:
                print(
                    f"  [{chart_id}] {field} mismatch: expected {new_hash}, "
                    f"got {current_hash} — skipping (may have been reverted)"
                )
                skipped += 1
                continue

            safe_to_delete.append(change)

    print(f"  {len(safe_to_delete)} old objects to delete, {skipped} skipped")

    if not safe_to_delete:
        print("Nothing to commit.")
        await pool.close()
        return

    semaphore = asyncio.Semaphore(CONCURRENCY)
    deleted = 0
    errors = 0

    async def delete_one(change, bucket):
        nonlocal deleted, errors
        async with semaphore:
            chart_id = change["chart_id"]
            author = change["author"]
            old_hash = change["old_hash"]

            try:
                old_key = f"{author}/{chart_id}/{old_hash}"
                obj = await bucket.Object(old_key)
                await obj.delete()
                deleted += 1
                print(f"  [{chart_id}] deleted old: {old_hash}")
            except Exception as e:
                errors += 1
                print(f"  [{chart_id}] ERROR: {e}")

    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(bucket_name)
        tasks = [delete_one(change, bucket) for change in safe_to_delete]
        await asyncio.gather(*tasks)

    print(f"\nDone! Deleted: {deleted}, Errors: {errors}")

    await pool.close()


asyncio.run(main())
