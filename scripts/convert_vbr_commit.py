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

    # pre-filter and group by field
    by_field: dict[str, list[dict]] = {}
    skipped = 0
    for change in changes:
        if (
            change["field"] not in ALLOWED_FIELDS
            or change["new_hash"] == change["old_hash"]
        ):
            skipped += 1
            continue
        by_field.setdefault(change["field"], []).append(change)

    # batch verify: one query per field
    print("Verifying DB state...")
    chart_current: dict[tuple[str, str], str] = {}  # (chart_id, field) -> current hash

    async def verify_field(field: str, field_changes: list[dict]):
        chart_ids = list({c["chart_id"] for c in field_changes})
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, {field} FROM charts WHERE id = ANY($1::text[])",
                chart_ids,
            )
        for row in rows:
            chart_current[(row["id"], field)] = row[field]

    await asyncio.gather(
        *[
            verify_field(field, field_changes)
            for field, field_changes in by_field.items()
        ]
    )

    safe_to_delete: list[dict] = []
    for field, field_changes in by_field.items():
        for change in field_changes:
            cid = change["chart_id"]
            key = (cid, field)
            if key not in chart_current:
                print(f"  [{cid}] chart not found, skipping")
                skipped += 1
                continue
            if chart_current[key] != change["new_hash"]:
                print(
                    f"  [{cid}] {field} mismatch: expected {change['new_hash']}, "
                    f"got {chart_current[key]} — skipping (may have been reverted)"
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
    done = 0

    async def delete_one(change, bucket):
        nonlocal deleted, errors, done
        async with semaphore:
            chart_id = change["chart_id"]
            author = change["author"]
            old_hash = change["old_hash"]

            try:
                old_key = f"{author}/{chart_id}/{old_hash}"
                obj = await bucket.Object(old_key)
                await obj.delete()
                deleted += 1
            except Exception as e:
                errors += 1
                print(f"  [{chart_id}] ERROR: {e}")
            done += 1
            if done % 10 == 0:
                print(f"  {done}/{len(safe_to_delete)} deleted...", end="\r")

    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(bucket_name)
        tasks = [delete_one(change, bucket) for change in safe_to_delete]
        await asyncio.gather(*tasks)

    print(f"\nDone! Deleted: {deleted}, Errors: {errors}")

    await pool.close()


asyncio.run(main())
