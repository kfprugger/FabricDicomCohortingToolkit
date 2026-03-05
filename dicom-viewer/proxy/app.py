"""
DICOMweb proxy — serves DICOM files from OneLake just-in-time via Flask.
Runs as a container on Azure Container Apps.
"""

import io
import json
import logging
import os
import zipfile
from functools import lru_cache

from flask import Flask, Response, jsonify, request
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pydicom
from pydicom.valuerep import PersonName

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
INDEX_PATH = os.environ.get("DICOM_INDEX_PATH", "/app/dicom_index.json")
_index: dict = {}
_datalake_client = None


def load_index():
    global _index
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            _index = json.load(f)
        logging.info(f"Loaded index: {len(_index)} studies")
    else:
        logging.warning(f"Index not found: {INDEX_PATH}")


def get_datalake_client() -> DataLakeServiceClient:
    global _datalake_client
    if _datalake_client is None:
        try:
            cred = ManagedIdentityCredential()
        except Exception:
            cred = DefaultAzureCredential()
        _datalake_client = DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=cred,
        )
    return _datalake_client


def parse_abfss(path: str):
    without_scheme = path.replace("abfss://", "")
    ws = without_scheme.split("@")[0]
    rest = without_scheme.split("/", 1)[1]
    parts = rest.split("/", 1)
    return ws, parts[0], parts[1] if len(parts) > 1 else ""


@lru_cache(maxsize=256)
def fetch_dcm(abfss_path: str) -> bytes:
    ws, item, fpath = parse_abfss(abfss_path)
    client = get_datalake_client()
    fs = client.get_file_system_client(ws)
    fc = fs.get_file_client(f"{item}/{fpath}")
    data = fc.download_file().readall()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            if not name.endswith("/"):
                return zf.read(name)
    return data


def build_meta(inst: dict) -> dict:
    """Build minimal DICOM JSON metadata from index (for QIDO search results)."""
    return {
        "00080016": {"vr": "UI", "Value": [inst.get("sopClassUid", "")]},
        "00080018": {"vr": "UI", "Value": [inst["sopInstanceUid"]]},
        "00080060": {"vr": "CS", "Value": [inst.get("modality", "")]},
        "0020000D": {"vr": "UI", "Value": [inst["studyInstanceUid"]]},
        "0020000E": {"vr": "UI", "Value": [inst["seriesInstanceUid"]]},
    }


# VR types that should be represented as numbers in DICOM JSON
_NUMERIC_VRS = {"DS", "FL", "FD", "IS", "SL", "SS", "UL", "US"}


def _dcm_element_to_json(elem):
    """Convert a single pydicom DataElement to DICOM JSON format."""
    vr = elem.VR
    if vr == "SQ":
        # Sequence: recursively convert
        items = []
        if elem.value:
            for seq_item in elem.value:
                item_dict = {}
                for sub_elem in seq_item:
                    tag = f"{sub_elem.tag.group:04X}{sub_elem.tag.element:04X}"
                    item_dict[tag] = _dcm_element_to_json(sub_elem)
                items.append(item_dict)
        return {"vr": vr, "Value": items}
    elif vr in ("OB", "OW", "OF", "OD", "UN"):
        # Binary data — skip (pixel data etc.)
        return {"vr": vr}
    elif elem.value is None or elem.value == "":
        return {"vr": vr}
    else:
        # Convert value to list
        if elem.VM > 1:
            values = list(elem.value)
        else:
            values = [elem.value]

        # Convert types
        converted = []
        for v in values:
            if isinstance(v, PersonName):
                converted.append({"Alphabetic": str(v)})
            elif vr in _NUMERIC_VRS:
                try:
                    if vr in ("FL", "FD", "DS"):
                        converted.append(float(v))
                    else:
                        converted.append(int(v))
                except (ValueError, TypeError):
                    converted.append(str(v))
            else:
                converted.append(str(v))

        return {"vr": vr, "Value": converted}


@lru_cache(maxsize=128)
def build_full_metadata(abfss_path: str) -> dict:
    """Fetch DICOM from OneLake, parse with pydicom, return full DICOM JSON metadata."""
    dcm_bytes = fetch_dcm(abfss_path)
    ds = pydicom.dcmread(io.BytesIO(dcm_bytes), stop_before_pixels=True)
    meta = {}

    # Include all file meta information (TransferSyntaxUID, SOP Class, etc.)
    if hasattr(ds, "file_meta"):
        for elem in ds.file_meta:
            tag = f"{elem.tag.group:04X}{elem.tag.element:04X}"
            try:
                meta[tag] = _dcm_element_to_json(elem)
            except Exception:
                pass

    # Include dataset elements (skip pixel data and large binary)
    for elem in ds:
        if elem.tag.group == 0x7FE0:  # Pixel Data group
            continue
        tag = f"{elem.tag.group:04X}{elem.tag.element:04X}"
        try:
            meta[tag] = _dcm_element_to_json(elem)
        except Exception:
            pass  # Skip problematic elements

    return meta


def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    return resp


@app.before_request
def handle_options():
    if request.method == "OPTIONS":
        return cors(Response("", 204))


@app.after_request
def add_cors(resp):
    return cors(resp)


# QIDO-RS: Search for studies
@app.route("/dicom-web/studies", methods=["GET"])
def search_studies():
    uid_filter = request.args.get("StudyInstanceUID")
    results = []
    for uid, instances in _index.items():
        if uid_filter and uid != uid_filter:
            continue
        if instances:
            f = instances[0]
            results.append({
                "0020000D": {"vr": "UI", "Value": [uid]},
                "00080060": {"vr": "CS", "Value": [f.get("modality", "")]},
                "00201206": {"vr": "IS", "Value": [str(len(set(i["seriesInstanceUid"] for i in instances)))]},
                "00201208": {"vr": "IS", "Value": [str(len(instances))]},
            })
    return Response(json.dumps(results), mimetype="application/dicom+json")


# WADO-RS: Study-level metadata (parses actual DICOM files via pydicom)
@app.route("/dicom-web/studies/<study_uid>/metadata", methods=["GET"])
def study_metadata(study_uid):
    if study_uid not in _index:
        return Response(json.dumps([]), mimetype="application/dicom+json")
    meta = []
    for inst in _index[study_uid]:
        try:
            m = build_full_metadata(inst["filePath"])
            meta.append(m)
        except Exception as e:
            logging.error(f"Failed to get metadata for {inst['sopInstanceUid']}: {e}")
            meta.append(build_meta(inst))
    return Response(json.dumps(meta), mimetype="application/dicom+json")


# QIDO-RS: Search for series within a study
@app.route("/dicom-web/studies/<study_uid>/series", methods=["GET"])
def search_series(study_uid):
    if study_uid not in _index:
        return Response(json.dumps([]), mimetype="application/dicom+json")
    series = {}
    for i in _index[study_uid]:
        sid = i["seriesInstanceUid"]
        if sid not in series:
            series[sid] = {"mod": i.get("modality", ""), "n": 0}
        series[sid]["n"] += 1
    results = [
        {
            "0020000D": {"vr": "UI", "Value": [study_uid]},
            "0020000E": {"vr": "UI", "Value": [sid]},
            "00080060": {"vr": "CS", "Value": [info["mod"]]},
            "00201209": {"vr": "IS", "Value": [str(info["n"])]},
        }
        for sid, info in series.items()
    ]
    return Response(json.dumps(results), mimetype="application/dicom+json")


# WADO-RS: Series-level metadata (parses actual DICOM files via pydicom)
@app.route("/dicom-web/studies/<study_uid>/series/<series_uid>/metadata", methods=["GET"])
def series_metadata(study_uid, series_uid):
    if study_uid not in _index:
        return Response(json.dumps([]), mimetype="application/dicom+json")
    meta = []
    for inst in _index[study_uid]:
        if inst["seriesInstanceUid"] == series_uid:
            try:
                m = build_full_metadata(inst["filePath"])
                meta.append(m)
            except Exception as e:
                logging.error(f"Failed metadata for {inst['sopInstanceUid']}: {e}")
                meta.append(build_meta(inst))
    return Response(json.dumps(meta), mimetype="application/dicom+json")


# QIDO-RS: Search for instances within a series
@app.route("/dicom-web/studies/<study_uid>/series/<series_uid>/instances", methods=["GET"])
def search_instances(study_uid, series_uid):
    if study_uid not in _index:
        return Response(json.dumps([]), mimetype="application/dicom+json")
    results = [build_meta(i) for i in _index[study_uid] if i["seriesInstanceUid"] == series_uid]
    return Response(json.dumps(results), mimetype="application/dicom+json")


# WADO-RS: Retrieve a single DICOM instance (pixel data or metadata)
@app.route("/dicom-web/studies/<study_uid>/series/<series_uid>/instances/<sop_uid>", methods=["GET"])
def retrieve_instance(study_uid, series_uid, sop_uid):
    target = _find_instance(study_uid, series_uid, sop_uid)
    if not target:
        return Response("Not found", 404)

    accept = request.headers.get("Accept", "")
    if "application/dicom+json" in accept:
        try:
            m = build_full_metadata(target["filePath"])
        except Exception:
            m = build_meta(target)
        return Response(json.dumps([m]), mimetype="application/dicom+json")

    dcm = fetch_dcm(target["filePath"])
    boundary = "----DICOMBoundary"
    body = f"--{boundary}\r\nContent-Type: application/dicom\r\n\r\n".encode() + dcm + f"\r\n--{boundary}--\r\n".encode()
    return Response(body, content_type=f'multipart/related; type="application/dicom"; boundary={boundary}')


# WADO-RS: Instance-level metadata
@app.route("/dicom-web/studies/<study_uid>/series/<series_uid>/instances/<sop_uid>/metadata", methods=["GET"])
def instance_metadata(study_uid, series_uid, sop_uid):
    target = _find_instance(study_uid, series_uid, sop_uid)
    if not target:
        return Response(json.dumps([]), mimetype="application/dicom+json")
    try:
        m = build_full_metadata(target["filePath"])
    except Exception:
        m = build_meta(target)
    return Response(json.dumps([m]), mimetype="application/dicom+json")


# WADO-RS: Retrieve pixel data frames
@app.route("/dicom-web/studies/<study_uid>/series/<series_uid>/instances/<sop_uid>/frames/<frames>", methods=["GET"])
def retrieve_frames(study_uid, series_uid, sop_uid, frames):
    target = _find_instance(study_uid, series_uid, sop_uid)
    if not target:
        return Response("Not found", 404)
    dcm = fetch_dcm(target["filePath"])
    boundary = "----DICOMBoundary"
    body = f"--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n".encode() + dcm + f"\r\n--{boundary}--\r\n".encode()
    return Response(body, content_type=f'multipart/related; type="application/octet-stream"; boundary={boundary}')


# Health check endpoint
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "studies": len(_index)})


def _find_instance(study_uid, series_uid, sop_uid):
    if study_uid not in _index:
        return None
    for i in _index[study_uid]:
        if i["seriesInstanceUid"] == series_uid and i["sopInstanceUid"] == sop_uid:
            return i
    return None


load_index()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
