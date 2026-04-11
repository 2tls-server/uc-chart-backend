import asyncio
import json
from pathlib import Path

import aioboto3
import asyncpg
import yaml

# S3 KEY STRUCTURE
# chart files:  {author}/{chart_id}/{hash}
# replays:      {author}/{chart_id}/replays/{submitter}/{hash}
# profile:      {account}/profile/{hash}  (+_webp variant)
# banner:       {account}/banner/{hash}   (+_webp variant)

HASH_COLS = (
    "jacket_file_hash",
    "music_file_hash",
    "chart_file_hash",
    "background_v1_file_hash",
    "background_v3_file_hash",
    "preview_file_hash",
    "background_file_hash",
)

VBR_CHANGES_FILE = Path("scripts/vbr_changes.json")
S3_PREFIX_CONCURRENCY = 32


async def fetch_db(config: dict):
    psql = config["psql"]
    pool = await asyncpg.create_pool(
        host=psql["host"],
        user=psql["user"],
        database=psql["database"],
        password=psql["password"],
        port=psql["port"],
        ssl="disable",
        min_size=3,
        max_size=3,
    )

    async def q_charts():
        async with pool.acquire() as conn:
            return await conn.fetch(
                f"""
                SELECT author, id, {', '.join(HASH_COLS)} FROM charts;
            """
            )

    async def q_replays():
        async with pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT c.author, l.chart_id, l.submitter,
                       l.replay_data_hash, l.replay_config_hash
                FROM leaderboards l JOIN charts c ON l.chart_id = c.id;
            """
            )

    async def q_accounts():
        async with pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT sonolus_id, profile_hash, banner_hash FROM accounts;
            """
            )

    print("Querying DB...")
    chart_rows, replay_rows, account_rows = await asyncio.gather(
        q_charts(), q_replays(), q_accounts()
    )
    await pool.close()

    expected_keys: set[str] = set()
    chart_hashes: dict[tuple[str, str], set[str]] = {}
    known_charts: set[tuple[str, str]] = set()
    known_accounts: set[str] = set()

    for row in chart_rows:
        author, cid = row["author"], row["id"]
        known_charts.add((author, cid))
        hashes: set[str] = set()
        for col in HASH_COLS:
            h = row[col]
            if h:
                expected_keys.add(f"{author}/{cid}/{h}")
                hashes.add(h)
        chart_hashes[(author, cid)] = hashes

    for row in replay_rows:
        author, cid, sub = row["author"], row["chart_id"], row["submitter"]
        expected_keys.add(f"{author}/{cid}/replays/{sub}/{row['replay_data_hash']}")
        expected_keys.add(f"{author}/{cid}/replays/{sub}/{row['replay_config_hash']}")

    for row in account_rows:
        sid = row["sonolus_id"]
        known_accounts.add(sid)
        if row["profile_hash"]:
            expected_keys.add(f"{sid}/profile/{row['profile_hash']}")
            expected_keys.add(f"{sid}/profile/{row['profile_hash']}_webp")
        if row["banner_hash"]:
            expected_keys.add(f"{sid}/banner/{row['banner_hash']}")
            expected_keys.add(f"{sid}/banner/{row['banner_hash']}_webp")

    print(
        f"  {len(chart_rows)} charts, {len(replay_rows)} replays, {len(account_rows)} accounts"
    )
    return expected_keys, chart_hashes, known_charts, known_accounts


async def scan_s3(config: dict) -> set[str]:
    s3_config = config["s3"]
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access-key-id"],
        aws_secret_access_key=s3_config["secret-access-key"],
        region_name=s3_config.get("location"),
    )
    bucket_name = s3_config["bucket-name"]

    print("Listing S3 top-level prefixes...")
    prefixes: list[str] = []
    async with session.client("s3", endpoint_url=s3_config["endpoint"]) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                prefixes.append(p["Prefix"])

    print(
        f"  {len(prefixes)} prefixes, scanning with {S3_PREFIX_CONCURRENCY} workers..."
    )
    actual_keys: set[str] = set()
    scanned = 0
    done_prefixes = 0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(S3_PREFIX_CONCURRENCY)

    async def scan_prefix(prefix: str):
        nonlocal scanned, done_prefixes
        keys: list[str] = []
        async with sem:
            async with session.client(
                "s3", endpoint_url=s3_config["endpoint"]
            ) as client:
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        keys.append(obj["Key"])
        async with lock:
            actual_keys.update(keys)
            scanned += len(keys)
            done_prefixes += 1
            if done_prefixes % 10 == 0:
                print(
                    f"  {done_prefixes}/{len(prefixes)} prefixes, {scanned} objects...",
                    end="\r",
                )

    await asyncio.gather(*[scan_prefix(p) for p in prefixes])
    print(f"  {done_prefixes}/{len(prefixes)} prefixes, {scanned} objects total.")
    return actual_keys


async def run():
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)

    (expected_keys, chart_hashes, known_charts, known_accounts), actual_keys = (
        await asyncio.gather(fetch_db(config), scan_s3(config))
    )

    # pre-vbr hashes
    pre_vbr_hashes: dict[tuple[str, str], set[str]] = {}
    if VBR_CHANGES_FILE.exists():
        with open(VBR_CHANGES_FILE, "r") as f:
            for change in json.load(f):
                key = (change["author"], change["chart_id"])
                pre_vbr_hashes.setdefault(key, set()).add(change["old_hash"])

    # DIFF
    raw_orphaned = actual_keys - expected_keys
    missing_s3 = expected_keys - actual_keys

    orphaned_s3: list[str] = []
    skipped = 0
    for key in sorted(raw_orphaned):
        parts = key.split("/")

        if len(parts) == 3 and "profile" not in parts and "banner" not in parts:
            author, cid, h = parts
            pair = (author, cid)
            if pair not in known_charts:
                skipped += 1
                continue
            vbr_old = pre_vbr_hashes.get(pair, set())
            if h in vbr_old and h in chart_hashes.get(pair, set()):
                skipped += 1
                continue

        elif len(parts) == 5 and parts[2] == "replays":
            if (parts[0], parts[1]) not in known_charts:
                skipped += 1
                continue

        elif len(parts) == 3 and parts[1] in ("profile", "banner"):
            if parts[0] not in known_accounts:
                skipped += 1
                continue

        orphaned_s3.append(key)

    print(f"\nDB expects {len(expected_keys)} keys, S3 has {len(actual_keys)} keys")
    print(f"Skipped {skipped} keys belonging to deleted charts/accounts")

    if missing_s3:
        print(f"\nMISSING S3 ({len(missing_s3)} DB entries with no S3 object):")
        for key in sorted(missing_s3):
            print(f"  {key}")
    else:
        print("No missing S3 keys.")

    if not orphaned_s3:
        print("No orphaned S3 keys to delete.")
        return

    print(f"\n{len(orphaned_s3)} orphaned S3 keys to delete.")

    s3_config = config["s3"]
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access-key-id"],
        aws_secret_access_key=s3_config["secret-access-key"],
        region_name=s3_config.get("location"),
    )
    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(s3_config["bucket-name"])
        # delete_objects takes max 1000 per batch
        for i in range(0, len(orphaned_s3), 1000):
            batch = [{"Key": k} for k in orphaned_s3[i : i + 1000]]
            await bucket.delete_objects(Delete={"Objects": batch})
            print(
                f"  deleted {min(i + 1000, len(orphaned_s3))}/{len(orphaned_s3)}...",
                end="\r",
            )

    print(f"  deleted {len(orphaned_s3)} orphaned S3 keys.")


if __name__ == "__main__":
    asyncio.run(run())
