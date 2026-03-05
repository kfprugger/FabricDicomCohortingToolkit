"""
Build a DICOM study index from Fabric ImagingMetastore.

Queries the ImagingMetastore table and produces a JSON index file mapping
studyInstanceUid to a list of instances with their OneLake file paths.

This index is used by the DICOMweb proxy container to serve DICOM files
from OneLake just-in-time (no pre-loading required).

Requirements:
    pip install pyodbc azure-identity

Usage:
    python build_index.py --output proxy/dicom_index.json
"""

import argparse
import json
import os
import struct
from collections import defaultdict

import pyodbc
from azure.identity import DefaultAzureCredential


FABRIC_SERVER = os.environ.get(
    "FABRIC_SERVER",
    "nkhahdl5to4ezo6p5bg76flepa-m7whz3hcli5edjmc6b5epuj6ze.datawarehouse.fabric.microsoft.com",
)
FABRIC_DB = os.environ.get("FABRIC_DB", "hds1_msft_silver")


def get_access_token() -> str:
    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    return token.token


def main():
    parser = argparse.ArgumentParser(
        description="Build DICOM study index from Fabric ImagingMetastore"
    )
    parser.add_argument(
        "--output",
        default="proxy/dicom_index.json",
        help="Output JSON index file (default: proxy/dicom_index.json)",
    )
    parser.add_argument("--server", default=FABRIC_SERVER)
    parser.add_argument("--database", default=FABRIC_DB)
    args = parser.parse_args()

    print(f"Connecting to {args.server}/{args.database}...")
    token = get_access_token()
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={args.server};"
        f"DATABASE={args.database};"
        f"Encrypt=Yes;",
        attrs_before={1256: token_struct},
    )

    print("Querying ImagingMetastore...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            m.studyInstanceUid,
            m.seriesInstanceUid,
            m.sopInstanceUid,
            m.filePath,
            m.msftSourceSystem,
            -- Get modality from the ImagingStudy table if available
            COALESCE(
                JSON_VALUE(i.series_string, '$[0].modality.code'),
                'OT'
            ) AS modality
        FROM dbo.ImagingMetastore m
        LEFT JOIN dbo.ImagingStudy i
            ON m.studyInstanceUid = JSON_VALUE(i.identifier_string, '$[0].value')
        WHERE m.filePath IS NOT NULL
    """)

    # Build index: studyUID → [instances]
    index = defaultdict(list)
    row_count = 0
    for row in cursor.fetchall():
        index[row.studyInstanceUid].append({
            "studyInstanceUid": row.studyInstanceUid,
            "seriesInstanceUid": row.seriesInstanceUid,
            "sopInstanceUid": row.sopInstanceUid,
            "filePath": row.filePath,
            "modality": row.modality,
        })
        row_count += 1

    conn.close()

    # Write index
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(dict(index), f, indent=2)

    index_size_kb = os.path.getsize(args.output) / 1024
    print(f"Done: {row_count} instances across {len(index)} studies")
    print(f"Index written to {args.output} ({index_size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
