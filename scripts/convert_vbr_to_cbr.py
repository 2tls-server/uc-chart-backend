"""
Migrate existing VBR MP3 audio files to CBR in S3 and update the database.

Usage: python scripts/convert_vbr_to_cbr.py
  Run from the project root (needs config.yml).

  --dry-run    Show what would be converted without making changes.
               Saves the first VBR file and its CBR conversion to temp
               files for manual inspection. Also estimates total time.

Changes are saved to scripts/vbr_changes.json for use with:
  - scripts/convert_vbr_revert.py   (revert: restore old hashes, delete new S3 objects)
  - scripts/convert_vbr_commit.py   (commit: delete old S3 objects)
"""

import sys
import os
import io
import json
import time
import tempfile
import yaml
import asyncio
import asyncpg
import aioboto3
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, ".")
from helpers.audio import ensure_cbr_mp3
from helpers.hashing import calculate_sha1

CONCURRENCY = 8  # max charts processed in parallel
WORKERS = 4  # ffmpeg subprocess pool size
CHANGES_FILE = "scripts/vbr_changes.json"

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DRY_RUN = "--dry-run" in sys.argv


def convert_and_hash(audio_bytes: bytes) -> tuple[bytes, str | None]:
    new_bytes = ensure_cbr_mp3(audio_bytes)
    if new_bytes is audio_bytes:
        return audio_bytes, None
    return new_bytes, calculate_sha1(new_bytes)


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {secs}s"


def print_progress(counters, total: int, start_time: float):
    done = counters["converted_audio"] + counters["skipped"] + counters["errors"]
    elapsed = time.time() - start_time
    pct = (done / total * 100) if total else 100

    bar_width = 30
    filled = int(bar_width * done / total) if total else bar_width
    bar = "█" * filled + "-" * (bar_width - filled)

    eta_str = ""
    if done > 0 and done < total:
        eta = elapsed / done * (total - done)
        eta_str = f" ETA {format_time(eta)}"

    print(
        f"\r  [{bar}] {done}/{total} ({pct:.1f}%) "
        f"elapsed {format_time(elapsed)}{eta_str}    ",
        end="",
        flush=True,
    )


async def process_chart(
    chart, bucket, pool, executor, semaphore, loop, counters, total, start_time, changes
):
    async with semaphore:
        chart_id = chart["id"]
        author = chart["author"]
        music_hash = chart["music_file_hash"]
        # preview_hash = chart["preview_file_hash"]

        # audio / BGM
        if music_hash:
            s3_key = f"{author}/{chart_id}/{music_hash}"
            try:
                obj = await bucket.Object(s3_key)
                resp = await obj.get()
                audio_bytes = await resp["Body"].read()

                new_bytes, new_hash = await loop.run_in_executor(
                    executor, convert_and_hash, audio_bytes
                )

                if new_hash is None:
                    counters["skipped"] += 1
                else:
                    counters["converted_audio"] += 1
                    new_key = f"{author}/{chart_id}/{new_hash}"
                    print(
                        f"\n  [{chart_id}] audio: VBR -> CBR "
                        f"({len(audio_bytes)} -> {len(new_bytes)} bytes, "
                        f"hash {music_hash} -> {new_hash})"
                    )

                    if DRY_RUN and not counters["sample_saved"]:
                        counters["sample_saved"] = True
                        sample_dir = os.path.join(
                            tempfile.gettempdir(), "vbr_to_cbr_sample"
                        )
                        os.makedirs(sample_dir, exist_ok=True)
                        vbr_path = os.path.join(
                            sample_dir, f"{chart_id}_original_vbr.mp3"
                        )
                        cbr_path = os.path.join(
                            sample_dir, f"{chart_id}_converted_cbr.mp3"
                        )
                        with open(vbr_path, "wb") as f:
                            f.write(audio_bytes)
                        with open(cbr_path, "wb") as f:
                            f.write(new_bytes)
                        print(f"\n  Sample saved for manual check:")
                        print(f"    VBR: {vbr_path}")
                        print(f"    CBR: {cbr_path}")

                    if not DRY_RUN:
                        # upload new CBR file
                        await bucket.upload_fileobj(
                            Fileobj=io.BytesIO(new_bytes),
                            Key=new_key,
                            ExtraArgs={"ContentType": "audio/mpeg"},
                        )
                        # update db hash (old S3 object kept for now)
                        async with pool.acquire() as conn:
                            await conn.execute(
                                "UPDATE charts SET music_file_hash = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                                new_hash,
                                chart_id,
                            )
                        # record change
                        changes.append(
                            {
                                "chart_id": chart_id,
                                "author": author,
                                "field": "music_file_hash",
                                "old_hash": music_hash,
                                "new_hash": new_hash,
                            }
                        )
            except Exception as e:
                counters["errors"] += 1
                print(f"\n  [{chart_id}] audio ERROR: {e}")
        else:
            counters["skipped"] += 1

        # Previews
        # if preview_hash:
        #     s3_key = f"{author}/{chart_id}/{preview_hash}"
        #     try:
        #         obj = await bucket.Object(s3_key)
        #         resp = await obj.get()
        #         preview_bytes = await resp["Body"].read()
        #
        #         new_bytes, new_hash = await loop.run_in_executor(
        #             executor, convert_and_hash, preview_bytes
        #         )
        #
        #         if new_hash is None:
        #             pass
        #         else:
        #             counters["converted_preview"] += 1
        #             new_key = f"{author}/{chart_id}/{new_hash}"
        #             print(
        #                 f"\n  [{chart_id}] preview: VBR -> CBR "
        #                 f"({len(preview_bytes)} -> {len(new_bytes)} bytes, "
        #                 f"hash {preview_hash} -> {new_hash})"
        #             )
        #
        #             if not DRY_RUN:
        #                 await bucket.upload_fileobj(
        #                     Fileobj=io.BytesIO(new_bytes),
        #                     Key=new_key,
        #                     ExtraArgs={"ContentType": "audio/mpeg"},
        #                 )
        #                 async with pool.acquire() as conn:
        #                     await conn.execute(
        #                         "UPDATE charts SET preview_file_hash = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
        #                         new_hash,
        #                         chart_id,
        #                     )
        #                 changes.append({
        #                     "chart_id": chart_id,
        #                     "author": author,
        #                     "field": "preview_file_hash",
        #                     "old_hash": preview_hash,
        #                     "new_hash": new_hash,
        #                 })
        #     except Exception as e:
        #         counters["errors"] += 1
        #         print(f"\n  [{chart_id}] preview ERROR: {e}")

        print_progress(counters, total, start_time)


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

    async with pool.acquire() as conn:
        charts = await conn.fetch(
            "SELECT id, author, music_file_hash, preview_file_hash FROM charts"
        )

    total = len(charts)
    print(f"Found {total} charts to check.")
    print(f"Concurrency: {CONCURRENCY} charts, {WORKERS} ffmpeg workers")
    if DRY_RUN:
        print("[DRY RUN] No changes will be made.\n")
    else:
        print()

    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    start_time = time.time()
    changes = []
    counters = {
        "converted_audio": 0,
        # "converted_preview": 0,
        "skipped": 0,
        "errors": 0,
        "sample_saved": False,
    }

    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(bucket_name)

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            tasks = [
                process_chart(
                    chart,
                    bucket,
                    pool,
                    executor,
                    semaphore,
                    loop,
                    counters,
                    total,
                    start_time,
                    changes,
                )
                for chart in charts
            ]
            await asyncio.gather(*tasks)

    elapsed = time.time() - start_time

    # save changes log
    if not DRY_RUN and changes:
        with open(CHANGES_FILE, "w") as f:
            json.dump(changes, f, indent=2)
        print(f"\n  Changes saved to {CHANGES_FILE} ({len(changes)} entries)")
        print(f"  Run scripts/convert_vbr_commit.py to delete old S3 objects")
        print(f"  Run scripts/convert_vbr_revert.py to undo all changes")

    print()  # newline after progress bar
    prefix = "[DRY RUN] " if DRY_RUN else ""
    print(f"\n{prefix}Done in {format_time(elapsed)}!")
    print(f"  Audio converted: {counters['converted_audio']}")
    # print(f"  Preview converted: {counters['converted_preview']}")
    print(f"  Skipped (already CBR): {counters['skipped']}")
    print(f"  Errors: {counters['errors']}")

    if DRY_RUN and counters["converted_audio"] > 0:
        overhead_per_chart = 2.0
        extra = counters["converted_audio"] * overhead_per_chart / CONCURRENCY
        estimated = elapsed + extra
        print(f"\n  Estimated real run time: ~{format_time(estimated)}")
        print(
            f"    (current elapsed + ~{format_time(extra)} for S3 upload/delete & DB updates)"
        )

    await pool.close()


asyncio.run(main())
