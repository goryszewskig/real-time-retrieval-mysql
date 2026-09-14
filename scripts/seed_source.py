#!/usr/bin/env python3
"""
Bulk-load data/jobseekers_10000.csv into the MySQL source
(usersdb.users on mysql-source). Idempotent: rerunning it upserts.

Needs admin credentials because the benchmark's write user only has
SELECT/UPDATE on users.

Usage:
    python scripts/seed_source.py [--csv data/jobseekers_10000.csv]
"""

import argparse
import csv
import os
from datetime import datetime

import pymysql


BATCH_SIZE = 500

INSERT_SQL = """
    INSERT INTO users (
        id, name, email, company, job_title, location,
        skills, bio, experience, created_at, updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) AS new
    ON DUPLICATE KEY UPDATE
        name = new.name,
        email = new.email,
        company = new.company,
        job_title = new.job_title,
        location = new.location,
        skills = new.skills,
        bio = new.bio,
        experience = new.experience,
        created_at = new.created_at,
        updated_at = new.updated_at
"""


def get_env(name, default=None):
    value = os.getenv(name, default)

    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value


def parse_iso(value):
    """'2024-07-02T00:05:13Z' -> naive datetime for MySQL (UTC)."""

    if not value:
        return None

    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).replace(tzinfo=None)


def main():

    parser = argparse.ArgumentParser(
        description="Seed the MySQL source users table from CSV."
    )

    parser.add_argument(
        "--csv",
        default="data/jobseekers_10000.csv",
        help="Source CSV. Default: data/jobseekers_10000.csv",
    )

    args = parser.parse_args()

    connection = pymysql.connect(
        host=os.getenv("MYSQL_SOURCE_HOST", "localhost"),
        port=int(os.getenv("MYSQL_SOURCE_PORT", "3306")),
        user=get_env("MYSQL_SOURCE_ADMIN_USER", "root"),
        password=get_env(
            "MYSQL_SOURCE_ADMIN_PASSWORD", "rootpassword"
        ),
        database=os.getenv("MYSQL_SOURCE_DB", "usersdb"),
        autocommit=False,
    )

    with open(args.csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows):,} rows from {args.csv}")

    batch = []
    inserted = 0

    with connection.cursor() as cursor:

        for row in rows:

            batch.append(
                (
                    int(row["id"]),
                    row.get("name"),
                    row.get("email"),
                    row.get("company"),
                    row.get("job_title"),
                    row.get("location"),
                    row.get("skills"),
                    row.get("bio"),
                    int(row["experience"])
                    if row.get("experience")
                    else None,
                    parse_iso(row.get("created_at")),
                    parse_iso(row.get("updated_at")),
                )
            )

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(INSERT_SQL, batch)
                connection.commit()
                inserted += len(batch)
                print(f"Seeded {inserted:,} rows...")
                batch.clear()

        if batch:
            cursor.executemany(INSERT_SQL, batch)
            connection.commit()
            inserted += len(batch)

    connection.close()

    print(f"Done. {inserted:,} rows upserted into usersdb.users.")


if __name__ == "__main__":
    main()
