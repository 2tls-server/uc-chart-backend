"""
Dump the PostgreSQL database to a file.

Usage: python scripts/db_dump.py [output_file]
  Run from the project root (needs config.yml).
  Default output: scripts/dumps/dump_YYYY-MM-DD_HHMMSS.sql
"""

import sys
import os
import subprocess
from datetime import datetime

import yaml

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

psql = config["psql"]

dump_dir = "scripts/dumps"
os.makedirs(dump_dir, exist_ok=True)

if len(sys.argv) > 1:
    output_file = sys.argv[1]
else:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_file = os.path.join(dump_dir, f"dump_{timestamp}.sql")

env = os.environ.copy()
env["PGPASSWORD"] = psql["password"]

cmd = [
    "pg_dump",
    "-h",
    psql["host"],
    "-p",
    str(psql["port"]),
    "-U",
    psql["user"],
    "-d",
    psql["database"],
    "--no-owner",
    "--no-acl",
    "--exclude-schema=cron",
    "-f",
    output_file,
]

print(f"Dumping database '{psql['database']}' to {output_file}...")
result = subprocess.run(cmd, env=env)

if result.returncode == 0:
    size = os.path.getsize(output_file)
    print(f"Done! ({size / 1024 / 1024:.1f} MB)")
else:
    print(f"pg_dump failed with exit code {result.returncode}")
    sys.exit(1)
