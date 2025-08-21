import os
import time
import json
from typing import Iterable, Optional

import boto3

# ---------- Settings / Env ----------
S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET") or "gofinfi-portal"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
S3_KMS_KEY_ID = os.getenv("S3_KMS_KEY_ID")  # optional, for SSE-KMS

# Layout & behavior toggles
NUMBERED_LAYOUT = (os.getenv("NUMBERED_LAYOUT", "false").lower() == "true")  # default: classic
CREATE_KEEP_FILES = (os.getenv("CREATE_KEEP_FILES", "true").lower() == "true")  # default: create .keep
SEED_SAMPLE_FILES = (os.getenv("SEED_SAMPLE_FILES", "false").lower() == "true")  # off by default

# S3 client
s3 = boto3.client("s3", region_name=AWS_REGION)


# ---------- Helpers ----------
def fmt_acid_display(acid_raw: str) -> str:
    """
    Format raw 9-char base62 acid into 'acid-XXX-XX-XXXX' display string.
    Ex: 'j6OUbHRve' -> 'acid-j6O-Ub-HRve'
    """
    if not acid_raw or len(acid_raw) < 9:
        # Fallback: if a display-like acid was passed, return as-is
        return acid_raw if acid_raw.startswith("acid-") else acid_raw
    return f"acid-{acid_raw[0:3]}-{acid_raw[3:5]}-{acid_raw[5:9]}"


def _sse_kwargs() -> dict:
    if S3_KMS_KEY_ID:
        return {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": S3_KMS_KEY_ID}
    # Default to AES256 for at-rest encryption even without KMS
    return {"ServerSideEncryption": "AES256"}


def _tagging(acid: str, org_id: Optional[str]) -> str:
    parts = [f"acid={acid}"]
    if org_id:
        parts.append(f"org={org_id}")
    # S3 expects query-string style: "k=v&k2=v2"
    return "&".join(parts)


def put_empty(key: str, *, acid: str, org_id: Optional[str]) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=b"",
        Tagging=_tagging(acid, org_id),
        **_sse_kwargs(),
    )


def put_text(key: str, text: str, *, acid: str, org_id: Optional[str], content_type: str = "text/plain") -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
        Tagging=_tagging(acid, org_id),
        **_sse_kwargs(),
    )


def put_bytes(key: str, data: bytes, *, acid: str, org_id: Optional[str], content_type: Optional[str] = None) -> None:
    kwargs = dict(Bucket=S3_BUCKET, Key=key, Body=data, Tagging=_tagging(acid, org_id))
    if content_type:
        kwargs["ContentType"] = content_type
    kwargs.update(_sse_kwargs())
    s3.put_object(**kwargs)


def put_json(key: str, obj: dict, *, acid: str, org_id: Optional[str]) -> None:
    put_text(key, json.dumps(obj, separators=(",", ":"), ensure_ascii=False), acid=acid, org_id=org_id, content_type="application/json")


def _prefix_for(name: str, level: str, numbered: bool) -> str:
    """Return correct folder prefix depending on classic vs numbered layout."""
    return f"{level}-{name}/" if numbered else f"{name}/"


def _folders_top(base: str, numbered: bool) -> dict:
    """
    Return a mapping of logical top-level names -> absolute S3 prefix under account base.
    """
    return {
        "_meta":        base + _prefix_for("_meta", "00", numbered),
        "uploads":      base + _prefix_for("uploads", "10", numbered),
        "intake":       base + _prefix_for("intake", "20", numbered),
        "profiles":     base + _prefix_for("profiles", "30", numbered),
        "applications": base + _prefix_for("applications", "50", numbered),
        "exports":      base + _prefix_for("exports", "60", numbered),
        "shared":       base + _prefix_for("shared", "70", numbered),
        "worklog":      base + _prefix_for("worklog", "80", numbered),
        "archive":      base + _prefix_for("archive", "90", numbered),
    }


def _ensure_folders(prefixes: Iterable[str], *, acid: str, org_id: Optional[str]) -> None:
    for p in prefixes:
        # create the "folder" marker
        if not p.endswith("/"):
            p = p + "/"
        put_empty(p, acid=acid, org_id=org_id)
        # drop a .keep to make folder visible in all UIs
        if CREATE_KEEP_FILES:
            put_empty(p + ".keep", acid=acid, org_id=org_id)


# ---------- Seeding (optional) ----------
def _seed_sample_files(tops: dict, *, bucket_url_base: str, acid_raw: str, acid_disp: str, org_id: Optional[str]) -> None:
    """
    Create the exact sample structure/files provided by the user.
    """
    # Applications
    apps = tops["applications"]
    put_bytes(apps + "package.pdf", b"%PDF-1.4\n% placeholder package\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(apps + "decision-letter.pdf", b"%PDF-1.4\n% placeholder decision\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")

    # Profiles
    profiles = tops["profiles"]
    put_text(profiles + "profiles.md", "# Profiles\n\nGlobal notes for all profiles.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")

    # Person: Don Draper
    dd = profiles + "person/Don Draper/"
    _ensure_folders([dd, dd + "documents/", dd + "vehicle/", dd + "device/"], acid=acid_raw, org_id=org_id)
    put_text(dd + "notes.md", "# Don Draper\n\nNotes go here.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    # documents
    put_bytes(dd + "documents/passport.pdf", b"%PDF-1.4\n% passport placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(dd + "documents/driver-license.pdf", b"%PDF-1.4\n% driver license placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(dd + "documents/utility-bill.pdf", b"%PDF-1.4\n% utility bill placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(dd + "documents/bank-statement-proof.pdf", b"%PDF-1.4\n% bank statement placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(dd + "documents/contract.pdf", b"%PDF-1.4\n% contract placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    # vehicle
    mustang = dd + "vehicle/Mustang-1965/"
    _ensure_folders([mustang], acid=acid_raw, org_id=org_id)
    put_text(mustang + "notes.md", "# Mustang 1965\n\nNotes.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_bytes(mustang + "title.pdf", b"%PDF-1.4\n% title placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(mustang + "insurance.pdf", b"%PDF-1.4\n% insurance placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    # device
    iphone = dd + "device/iPhone15/"
    _ensure_folders([iphone], acid=acid_raw, org_id=org_id)
    put_text(iphone + "notes.md", "# iPhone 15\n\nNotes.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_text(iphone + "imei.txt", "IMEI: 000000000000000\n", acid=acid_raw, org_id=org_id, content_type="text/plain")

    # Company: SterlingCooper
    sc = profiles + "company/SterlingCooper/"
    _ensure_folders([sc, sc + "legal/", sc + "financials/", sc + "domain/"], acid=acid_raw, org_id=org_id)
    put_text(sc + "notes.md", "# Sterling Cooper\n\nNotes.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    # legal
    put_bytes(sc + "legal/articles.pdf", b"%PDF-1.4\n% articles placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(sc + "legal/operating-agreement.pdf", b"%PDF-1.4\n% op agreement placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(sc + "legal/certificate-of-good-standing.pdf", b"%PDF-1.4\n% cert placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(sc + "legal/ein-verification-letter.pdf", b"%PDF-1.4\n% ein letter placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(sc + "legal/business-license.pdf", b"%PDF-1.4\n% business license placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(sc + "legal/bank-resolution.pdf", b"%PDF-1.4\n% bank resolution placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    # financials
    for fname in ["monthly-statement-2025-07.pdf", "monthly-statement-2025-06.pdf", "monthly-statement-2025-05.pdf", "tax-return-2023.pdf", "tax-return-2024.pdf"]:
        put_bytes(sc + f"financials/{fname}", b"%PDF-1.4\n% placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    # employees.xlsx (placeholder)
    put_text(sc + "employees.xlsx", "placeholder spreadsheet\n", acid=acid_raw, org_id=org_id, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    # domain
    domain = sc + "domain/sterlingcooper.com/"
    _ensure_folders([domain], acid=acid_raw, org_id=org_id)
    put_text(domain + "notes.md", "# sterlingcooper.com\n\nNotes.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_text(domain + "dns-records.txt", "A 1.2.3.4\nCNAME www -> apex\n", acid=acid_raw, org_id=org_id, content_type="text/plain")


# ---------- Public API ----------
def provision_account(acid_raw: str, org_id: Optional[str] = None) -> str:
    """
    Create the account scaffold at:
      a/{acid_display}/...
    Returns the base prefix created.
    """
    acid_disp = fmt_acid_display(acid_raw)
    base = f"a/{acid_disp}/"

    # Build top-level folders
    tops = _folders_top(base, NUMBERED_LAYOUT)
    _ensure_folders(tops.values(), acid=acid_raw, org_id=org_id)

    # _meta files
    now = int(time.time())
    links = {k: f"s3://{S3_BUCKET}/{v}" for k, v in tops.items()}
    account_json = {
        "acid": acid_raw,
        "acid_display": acid_disp,
        "org_id": org_id,
        "links": links,
        "layout": "numbered" if NUMBERED_LAYOUT else "classic",
        "version": 1,
        "created_at": now,
    }
    put_json(tops["_meta"] + "account.json", account_json, acid=acid_raw, org_id=org_id)
    put_json(tops["_meta"] + "relationships.json", {"people": [], "companies": []}, acid=acid_raw, org_id=org_id)

    # shared + worklog defaults
    put_text(tops["shared"] + "checklist.gdoc.link",
             json.dumps({"note": "Replace with a signed S3 URL or pointer to exports/"}),
             acid=acid_raw, org_id=org_id, content_type="application/json")
    put_text(tops["worklog"] + "progress-log.md", "# Progress Log\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_text(tops["worklog"] + "decisions.md", "# Decisions\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")

    # profiles.md (always create)
    put_text(tops["profiles"] + "profiles.md", "# Profiles\n\nLoose notes on profile changes.\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")

    # Optional sample seeding
    if SEED_SAMPLE_FILES:
        bucket_url_base = f"s3://{S3_BUCKET}/"
        _seed_sample_files(tops, bucket_url_base=bucket_url_base, acid_raw=acid_raw, acid_disp=acid_disp, org_id=org_id)

    return base
