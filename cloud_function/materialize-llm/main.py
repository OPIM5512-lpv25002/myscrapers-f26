# Combine Gemini JSONL outputs into a separate growing CSV.

import csv
import json
import os
import re
from datetime import datetime, timezone

from flask import Request, jsonify
from google.cloud import storage

BUCKET_NAME = os.getenv("GCS_BUCKET")
STRUCTURED_PREFIX = os.getenv("STRUCTURED_PREFIX", "structured")

storage_client = storage.Client()

RUN_ID_ISO_RE = re.compile(r"^\d{8}T\d{6}Z$")
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

CSV_COLUMNS = [
    "post_id", "run_id", "scraped_at",
    "price", "year", "make", "model", "mileage",
    "source_txt",
    "trim", "body_type", "drivetrain", "transmission",
    "fuel_type", "exterior_color", "title_status",
    "condition", "leather_seats", "city"
]


def _list_run_ids():
    iterator = storage_client.list_blobs(
        BUCKET_NAME,
        prefix=f"{STRUCTURED_PREFIX}/",
        delimiter="/"
    )
    for _ in iterator:
        pass

    run_ids = []
    for prefix in iterator.prefixes:
        tail = prefix.rstrip("/").split("/")[-1]
        if tail.startswith("run_id="):
            rid = tail.split("run_id=", 1)[1]
            if RUN_ID_ISO_RE.match(rid) or RUN_ID_PLAIN_RE.match(rid):
                run_ids.append(rid)
    return sorted(run_ids)


def _run_id_to_dt(rid):
    if RUN_ID_ISO_RE.match(rid):
        return datetime.strptime(
            rid, "%Y%m%dT%H%M%SZ"
        ).replace(tzinfo=timezone.utc)
    return datetime.strptime(
        rid, "%Y%m%d%H%M%S"
    ).replace(tzinfo=timezone.utc)


def materialize_http(request: Request):
    try:
        if not BUCKET_NAME:
            return jsonify({
                "ok": False,
                "error": "missing GCS_BUCKET env"
            }), 500

        bucket = storage_client.bucket(BUCKET_NAME)
        run_ids = _list_run_ids()
        latest_by_post = {}
        latest_time = {}
        records_read = 0

        for rid in run_ids:
            run_time = _run_id_to_dt(rid)
            prefix = f"{STRUCTURED_PREFIX}/run_id={rid}/jsonl_llm/"

            for blob in bucket.list_blobs(prefix=prefix):
                if not blob.name.endswith(".jsonl"):
                    continue

                for line in blob.download_as_text().splitlines():
                    if not line.strip():
                        continue

                    record = json.loads(line)
                    post_id = record.get("post_id")
                    if not post_id:
                        continue

                    records_read += 1
                    record.setdefault("run_id", rid)

                    if (
                        post_id not in latest_by_post
                        or run_time > latest_time[post_id]
                    ):
                        latest_by_post[post_id] = record
                        latest_time[post_id] = run_time

        if not latest_by_post:
            return jsonify({
                "ok": False,
                "error": "No Gemini records found",
                "runs_scanned": len(run_ids)
            }), 200

        final_key = (
            f"{STRUCTURED_PREFIX}/datasets/listings_master_llm.csv"
        )

        with bucket.blob(final_key).open("w") as output:
            writer = csv.DictWriter(
                output,
                fieldnames=CSV_COLUMNS,
                extrasaction="ignore"
            )
            writer.writeheader()
            for record in latest_by_post.values():
                writer.writerow({
                    column: record.get(column)
                    for column in CSV_COLUMNS
                })

        return jsonify({
            "ok": True,
            "runs_scanned": len(run_ids),
            "records_read": records_read,
            "unique_listings": len(latest_by_post),
            "rows_written": len(latest_by_post),
            "output_csv": f"gs://{BUCKET_NAME}/{final_key}"
        }), 200

    except Exception as error:
        return jsonify({
            "ok": False,
            "error": f"{type(error).__name__}: {error}"
        }), 500
