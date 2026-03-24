"""
Restore the PostgreSQL database from a dump file.

Usage: python scripts/db_restore.py <input_file>
  Run from the project root (needs config.yml).

WARNING: This will drop and recreate all tables in the target database.
"""

import sys
import os
import subprocess

import yaml

if len(sys.argv) < 2:
    print("Usage: python scripts/db_restore.py <input_file>")
    sys.exit(1)

input_file = sys.argv[1]
if not os.path.exists(input_file):
    print(f"File not found: {input_file}")
    sys.exit(1)

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

psql = config["psql"]

print(f"This will restore '{input_file}' into database '{psql['database']}'.")
print("WARNING: Existing data will be overwritten.")
confirm = input("Type 'yes' to continue: ")
if confirm.strip().lower() != "yes":
    print("Aborted.")
    sys.exit(0)

env = os.environ.copy()
env["PGPASSWORD"] = psql["password"]

# Drop and recreate all public tables before restoring
drop_cmd = [
    "psql",
    "-h",
    psql["host"],
    "-p",
    str(psql["port"]),
    "-U",
    psql["user"],
    "-d",
    psql["database"],
    "-c",
    "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
]

print("Dropping existing schema...")
result = subprocess.run(drop_cmd, env=env)
if result.returncode != 0:
    print(f"Schema drop failed with exit code {result.returncode}")
    sys.exit(1)

# Restore from dump
restore_cmd = [
    "psql",
    "-h",
    psql["host"],
    "-p",
    str(psql["port"]),
    "-U",
    psql["user"],
    "-d",
    psql["database"],
    "-f",
    input_file,
]

print(f"Restoring from {input_file}...")
result = subprocess.run(restore_cmd, env=env)

if result.returncode == 0:
    print("Done!")
else:
    print(f"Restore failed with exit code {result.returncode}")
    sys.exit(1)
