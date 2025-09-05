# api_v201.py
import os
import json
import random
import string
from typing import List, Optional
from datetime import datetime, timezone

import boto3
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

# ---- Environment ----
BUCKET = os.getenv("S3_BUCKET", "gofinfi-portal")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
CREATE_KEEP_FILES = os.getenv("CREATE_KEEP_FILES", "true").lower() == "true"
PROVISIONER_SECRET = os.getenv("PROVISIONER_SECRET")

s3 = boto3.client("s3", region_name=AWS_REGION)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _generate_acid_string() -> str:
    alphabet = string.ascii_letters + string.digits
    s1 = "".join(random.choices(alphabet, k=3))
    s2 = "".join(random.choices(alphabet, k=2))
    s3 = "".join(random.choices(alphabet, k=4))
    return s1 + s2 + s3

def _fmt_acid_display(acid: str) -> str:
    a = acid.strip()
    if a.startswith("acid-"):
        return a
    if len(a) >= 9:
        return f"acid-{a[0:3]}-{a[3:5]}-{a[5:9]}"
    return f"acid-{a}"


def _ensure_folder(prefix: str):
    # create a zero-byte "folder" object, and optional .keep to keep it visible
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    s3.put_object(Bucket=BUCKET, Key=prefix, Body=b"")
    if CREATE_KEEP_FILES:
        s3.put_object(Bucket=BUCKET, Key=prefix + ".keep", Body=b"")


def _put_text(key: str, text: str, content_type: str = "text/plain"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=text.encode("utf-8"), ContentType=content_type)


def _put_json(key: str, obj: dict):
    body = json.dumps(obj, indent=2)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")


def _base(acid_display: str) -> str:
    return f"a/{acid_display}/"


def _profiles_base(acid_display: str) -> str:
    return _base(acid_display) + "profiles/"


def _ensure_top(acid_display: str):
    base = _base(acid_display)
    for name in ["_meta/", "uploads/", "intake/", "profiles/", "applications/", "exports/", "shared/", "worklog/", "archive/"]:
        _ensure_folder(base + name)
    _put_text(base + "profiles/profiles.md", "# Profiles\n", "text/markdown")


def _discover_profiles(acid_display: str):
    """Inventory S3 to build people/companies/devices/vehicles/domains and return (profiles_summary, relationships)."""
    base = _profiles_base(acid_display)

    people = []
    companies = []
    devices = []
    vehicles = []
    domains = []

    # Persons
    person_root = base + "person/"
    r1 = s3.list_objects_v2(Bucket=BUCKET, Prefix=person_root, Delimiter="/")
    for pfx in r1.get("CommonPrefixes", []):
        person_path = pfx["Prefix"]
        pname = person_path.rstrip("/").split("/")[-1]
        people.append({ "name": pname, "path": person_path })

        # Devices
        dev_root = person_path + "device/"
        r2 = s3.list_objects_v2(Bucket=BUCKET, Prefix=dev_root, Delimiter="/")
        for sp in r2.get("CommonPrefixes", []):
            dpath = sp["Prefix"]
            dname = dpath.rstrip("/").split("/")[-1]
            devices.append({ "name": dname, "owner_type": "person", "owner_name": pname, "path": dpath })

        # Vehicles
        veh_root = person_path + "vehicle/"
        r3 = s3.list_objects_v2(Bucket=BUCKET, Prefix=veh_root, Delimiter="/")
        for sp in r3.get("CommonPrefixes", []):
            vpath = sp["Prefix"]
            vname = vpath.rstrip("/").split("/")[-1]
            vehicles.append({ "name": vname, "owner_type": "person", "owner_name": pname, "path": vpath })

    # Companies
    comp_root = base + "company/"
    r4 = s3.list_objects_v2(Bucket=BUCKET, Prefix=comp_root, Delimiter="/")
    for pfx in r4.get("CommonPrefixes", []):
        comp_path = pfx["Prefix"]
        cname = comp_path.rstrip("/").split("/")[-1]
        companies.append({ "name": cname, "path": comp_path })

        # Domains
        dom_root = comp_path + "domain/"
        r5 = s3.list_objects_v2(Bucket=BUCKET, Prefix=dom_root, Delimiter="/")
        for sp in r5.get("CommonPrefixes", []):
            dpath = sp["Prefix"]
            dname = dpath.rstrip("/").split("/")[-1]
            domains.append({ "name": dname, "owner_type": "company", "owner_name": cname, "path": dpath })

    profiles_summary = { "persons": people, "companies": companies }
    relationships = {
        "people": people,
        "companies": companies,
        "devices": devices,
        "vehicles": vehicles,
        "domains": domains,
        "generated_at": _iso_now(),
    }
    return profiles_summary, relationships


def _refresh_meta(acid: str, org_id: Optional[str], name: Optional[str]):
    """Recompute and write _meta/account.json + _meta/relationships.json based on current S3 contents."""
    acid_display = _fmt_acid_display(acid)
    base = _base(acid_display)
    meta = base + "_meta/"

    profiles_summary, relationships = _discover_profiles(acid_display)
    account = {
        "acid": acid.replace("acid-", ""),
        "acid_display": acid_display,
        "org_id": org_id,
        "name": name,
        "created_at": _iso_now(),  # refresh time
        "layout": "classic",
        "links": {
            "uploads": f"s3://{BUCKET}/{base}uploads/",
            "profiles": f"s3://{BUCKET}/{base}profiles/",
            "applications": f"s3://{BUCKET}/{base}applications/",
            "exports": f"s3://{BUCKET}/{base}exports/",
            "shared": f"s3://{BUCKET}/{base}shared/",
            "worklog": f"s3://{BUCKET}/{base}worklog/",
            "archive": f"s3://{BUCKET}/{base}archive/",
            "_meta": f"s3://{BUCKET}/{base}_meta/"
        },
        "profiles": profiles_summary,
    }
    _put_json(meta + "account.json", account)
    _put_json(meta + "relationships.json", relationships)
    return { "account": account, "relationships": relationships }


def _auth_or_401(secret: Optional[str]):
    if PROVISIONER_SECRET and secret != PROVISIONER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---- Router & Schemas ----
router = APIRouter()

class Person(BaseModel):
    name: str
    notes: Optional[str] = ""
    devices: Optional[List[str]] = []
    vehicles: Optional[List[str]] = []

class Company(BaseModel):
    name: str
    notes: Optional[str] = ""
    domains: Optional[List[str]] = []

class PersonReq(BaseModel):
    org_id: Optional[str] = None
    name: Optional[str] = None
    person: Person

class CompanyReq(BaseModel):
    org_id: Optional[str] = None
    name: Optional[str] = None
    company: Company

class MetaRefreshReq(BaseModel):
    org_id: Optional[str] = None
    name: Optional[str] = None

class UploadUrlReq(BaseModel):
    path: str
    content_type: Optional[str] = "application/octet-stream"
    expires_in: Optional[int] = 600

@router.get("/acid")
def get_acid():
    acid_base = _generate_acid_string()
    acid = f"{acid_base[0:3]}-{acid_base[3:5]}-{acid_base[5:9]}"
    acid_display = _fmt_acid_display(acid_base)
    return {"acid": acid, "acid_display": acid_display}


@router.post("/v201/accounts/{acid}/profiles/person")
def add_person(acid: str, payload: PersonReq, x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret")):
    _auth_or_401(x_internal_secret)
    acid_display = _fmt_acid_display(acid)

    # ensure top-level + person skeleton
    _ensure_top(acid_display)
    p = payload.person
    pname = p.name.strip() or "Person"
    person_base = _profiles_base(acid_display) + f"person/{pname}/"
    _ensure_folder(person_base)
    _put_text(person_base + "notes.md", p.notes or "", "text/markdown")
    _ensure_folder(person_base + "documents/")
    _ensure_folder(person_base + "device/")
    _ensure_folder(person_base + "vehicle/")

    for d in (p.devices or []):
        dbase = person_base + f"device/{d}/"
        _ensure_folder(dbase)
        _put_text(dbase + "notes.md", "", "text/markdown")

    for v in (p.vehicles or []):
        vbase = person_base + f"vehicle/{v}/"
        _ensure_folder(vbase)
        _put_text(vbase + "notes.md", "", "text/markdown")

    meta = _refresh_meta(acid_display, payload.org_id, payload.name)
    return { "ok": True, "action": "add_person", **meta }


@router.post("/v201/accounts/{acid}/profiles/company")
def add_company(acid: str, payload: CompanyReq, x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret")):
    _auth_or_401(x_internal_secret)
    acid_display = _fmt_acid_display(acid)

    _ensure_top(acid_display)
    c = payload.company
    cname = c.name.strip() or "Company"
    company_base = _profiles_base(acid_display) + f"company/{cname}/"
    _ensure_folder(company_base)
    _put_text(company_base + "notes.md", c.notes or "", "text/markdown")
    _ensure_folder(company_base + "legal/")
    _ensure_folder(company_base + "financials/")
    _ensure_folder(company_base + "domain/")

    for dom in (c.domains or []):
        dbase = company_base + f"domain/{dom}/"
        _ensure_folder(dbase)
        _put_text(dbase + "notes.md", "", "text/markdown")
        _put_text(dbase + "dns-records.txt", "", "text/plain")

    meta = _refresh_meta(acid_display, payload.org_id, payload.name)
    return { "ok": True, "action": "add_company", **meta }


@router.post("/v201/accounts/{acid}/meta/refresh")
def meta_refresh(acid: str, payload: MetaRefreshReq, x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret")):
    _auth_or_401(x_internal_secret)
    acid_display = _fmt_acid_display(acid)
    _ensure_top(acid_display)
    meta = _refresh_meta(acid_display, payload.org_id, payload.name)
    return { "ok": True, "action": "meta_refresh", **meta }


@router.post("/v201/accounts/{acid}/upload-url")
def get_upload_url(acid: str, payload: UploadUrlReq, x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret")):
    _auth_or_401(x_internal_secret)
    # Stub: your organization signs via app.openportal.to/api already—keep that centralized.
    return {
        "ok": False,
        "status": 501,
        "message": "Upload URL signing is handled by app.openportal.to/api per your existing setup.",
        "suggested_signer": "https://app.openportal.to/api",
        "requested": {"acid": acid, "path": payload.path, "content_type": payload.content_type, "expires_in": payload.expires_in}
    }
