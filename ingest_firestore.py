from __future__ import annotations

# import json
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from google.cloud import bigquery
from google.cloud import firestore


FIRESTORE_PROJECT_ID = "copy-wt-roster"
BIGQUERY_PROJECT_ID = "wt-roster-agentic"
BIGQUERY_DATASET = "RAW_TEST"

COLLECTIONS = {
    "members": "fs_members",
    "rosters": "fs_rosters",
    "blockouts": "fs_blockouts",
}


def make_json_safe(value: Any) -> Any:
    """Convert Firestore-specific values into JSON-compatible values."""

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, firestore.DocumentReference):
        return value.path

    if isinstance(value, dict):
        return {
            key: make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [make_json_safe(item) for item in value]

    return value


def extract_collection(
    client: firestore.Client,
    collection_name: str,
    batch_id: str,
    ingested_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    documents = client.collection(collection_name).stream()

    for document in documents:
        document_data = make_json_safe(document.to_dict())
        rows.append(
            {
                "document_id": document.id,
                "source_system": "firestore",
                "source_application": "wt_roster_app",
                "source_project": FIRESTORE_PROJECT_ID,
                "source_database": "(default)",
                "source_collection": collection_name,
                "data": document_data,
                "source_created_at": (
                    document.create_time.isoformat()
                    if document.create_time
                    else None
                ),
                "source_updated_at": (
                    document.update_time.isoformat()
                    if document.update_time
                    else None
                ),
                "ingested_at": ingested_at,
                "batch_id": batch_id,
            }
        )

    return rows


def load_to_bigquery(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    table_name: str,
) -> None:
    table_id = (
        f"{BIGQUERY_PROJECT_ID}."
        f"{BIGQUERY_DATASET}."
        f"{table_name}"
    )

    schema = [
        bigquery.SchemaField("document_id", "STRING", mode="REQUIRED"),
        # bigquery.SchemaField("document_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_application", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_database", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_collection", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data", "JSON"),
        bigquery.SchemaField("source_created_at", "TIMESTAMP"),
        bigquery.SchemaField("source_updated_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        rows,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    destination_table = client.get_table(table_id)

    print(
        f"Loaded {destination_table.num_rows} rows "
        f"into {table_id}"
    )


def main() -> None:
    firestore_client = firestore.Client(
        project=FIRESTORE_PROJECT_ID
    )

    bigquery_client = bigquery.Client(
        project=BIGQUERY_PROJECT_ID
    )

    batch_id = str(uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    print(f"Starting ingestion batch: {batch_id}")

    for collection_name, table_name in COLLECTIONS.items():
        print(f"Extracting Firestore collection: {collection_name}")

        rows = extract_collection(
            client=firestore_client,
            collection_name=collection_name,
            batch_id=batch_id,
            ingested_at=ingested_at,
        )

        print(f"Extracted {len(rows)} documents")

        load_to_bigquery(
            client=bigquery_client,
            rows=rows,
            table_name=table_name,
        )

    print(f"Completed ingestion batch: {batch_id}")


if __name__ == "__main__":
    main()