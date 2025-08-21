import os
import time
import json
from typing import Iterable, Optional, List, Dict, Tuple

import boto3

# ---------- Settings / Env ----------
S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET") or "gofinfi-portal"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
S3_KMS_KEY_ID = os.getenv("S3_KMS_KEY_ID")  # optional, for SSE-KMS

# Layout & behavior toggles
NUMBERED_LAYOUT = (os.getenv("NUMBERED_LAYOUT", "false").lower() == "true")  # default: classic
CREATE_KEEP_FILES = (os.getenv("CREATE_KEEP_FILES", "true").lower() == "true")  # default: create .keep
SEED_SAMPLE_FILES = (os.getenv("SEED_SAMPLE_FILES", "false").lower() == "true")  # legacy demo seeding (Don Draper etc.)

# S3 client
s3 = boto3.client("s3", region_name=AWS_REGION)


# ---------- Helpers ----------
def fmt_acid_display(acid_raw: str) -> str:
    """
    Turn a 9-char base62 acid into 'acid-XXX-XX-XXXX' display.
    If input already looks like 'acid-...', return as-is.
    """
    if not acid_raw:
        return acid_raw
    if acid_raw.startswith("acid-"):
        return acid_raw
    if len(acid_raw) >= 9:
        return f"acid-{acid_raw[0:3]}-{acid_raw[3:5]}-{acid_raw[5:9]}"
    return acid_raw


def to_raw_acid(acid_or_display: str) -> str:
    """
    Convert either raw (XXXXXXXXX) or display (acid-XXX-XX-XXXX) to raw 9-char.
    If cannot parse, returns the original.
    """
    if not acid_or_display:
        return acid_or_display
    if acid_or_display.startswith("acid-"):
        return acid_or_display.replace("acid-", "").replace("-", "")
    return acid_or_display


def normalize_acid(acid_or_display: str) -> Tuple[str, str]:
    raw = to_raw_acid(acid_or_display)
    disp = fmt_acid_display(raw if raw else acid_or_display)
    return raw, disp


def sanitize_name(name: str) -> str:
    """Basic sanitization for S3 keys (avoid slashes)."""
    return (name or "").replace("/", "-").replace("\\", "-").strip() or "Unnamed"


def _sse_kwargs() -> dict:
    if S3_KMS_KEY_ID:
        return {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": S3_KMS_KEY_ID}
    return {"ServerSideEncryption": "AES256"}


def _tagging(acid: str, org_id: Optional[str]) -> str:
    parts = [f"acid={acid}"]
    if org_id:
        parts.append(f"org={org_id}")
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
    """Mapping of top-level names -> absolute S3 prefix under account base."""
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
        if not p.endswith("/"):
            p = p + "/"
        put_empty(p, acid=acid, org_id=org_id)
        if CREATE_KEEP_FILES:
            put_empty(p + ".keep", acid=acid, org_id=org_id)


# ---------- Optional legacy demo seeding ----------
def _seed_sample_files_legacy(tops: dict, *, acid_raw: str, acid_disp: str, org_id: Optional[str]) -> None:
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
    for f in ["passport.pdf", "driver-license.pdf", "utility-bill.pdf", "bank-statement-proof.pdf", "contract.pdf"]:
        put_bytes(dd + f"documents/{f}", b"%PDF-1.4\n% placeholder\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    mustang = dd + "vehicle/Mustang-1965/"
    _ensure_folders([mustang], acid=acid_raw, org_id=org_id)
    put_text(mustang + "notes.md", "# Mustang 1965\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_bytes(mustang + "title.pdf", b"%PDF-1.4\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    put_bytes(mustang + "insurance.pdf", b"%PDF-1.4\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    iphone = dd + "device/iPhone15/"
    _ensure_folders([iphone], acid=acid_raw, org_id=org_id)
    put_text(iphone + "notes.md", "# iPhone 15\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_text(iphone + "imei.txt", "IMEI: 000000000000000\n", acid=acid_raw, org_id=org_id, content_type="text/plain")

    # Company: SterlingCooper
    sc = profiles + "company/SterlingCooper/"
    _ensure_folders([sc, sc + "legal/", sc + "financials/", sc + "domain/"], acid=acid_raw, org_id=org_id)
    put_text(sc + "notes.md", "# Sterling Cooper\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    for f in ["articles.pdf", "operating-agreement.pdf", "certificate-of-good-standing.pdf", "ein-verification-letter.pdf", "business-license.pdf", "bank-resolution.pdf"]:
        put_bytes(sc + f"legal/{f}", b"%PDF-1.4\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    for f in ["monthly-statement-2025-07.pdf", "monthly-statement-2025-06.pdf", "monthly-statement-2025-05.pdf", "tax-return-2023.pdf", "tax-return-2024.pdf"]:
        put_bytes(sc + f"financials/{f}", b"%PDF-1.4\n", acid=acid_raw, org_id=org_id, content_type="application/pdf")
    domain = sc + "domain/sterlingcooper.com/"
    _ensure_folders([domain], acid=acid_raw, org_id=org_id)
    put_text(domain + "notes.md", "# sterlingcooper.com\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
    put_text(domain + "dns-records.txt", "A 1.2.3.4\nCNAME www -> apex\n", acid=acid_raw, org_id=org_id, content_type="text/plain")


# ---------- v2 Profiles seeding from webhook ----------
def _seed_from_payload(tops: dict, payload: dict, *, acid_raw: str, org_id: Optional[str]) -> Dict[str, list]:
    """
    Create profiles based on optional 'persons' and 'companies' arrays in payload.
    Returns a summary dict with created profiles.
    """
    created = {"persons": [], "companies": []}
    profiles = tops["profiles"]

    persons: List[dict] = payload.get("persons") or []
    companies: List[dict] = payload.get("companies") or []

    # Persons
    for p in persons:
        pname = sanitize_name(p.get("name") or "Unnamed Person")
        root = f"{profiles}person/{pname}/"
        _ensure_folders([root, root + "documents/", root + "vehicle/", root + "device/"], acid=acid_raw, org_id=org_id)
        if p.get("notes"):
            put_text(root + "notes.md", f"# {pname}\n\n{p['notes']}\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        # optional sub-profiles
        for v in p.get("vehicles") or []:
            vname = sanitize_name(v)
            vroot = f"{root}vehicle/{vname}/"
            _ensure_folders([vroot], acid=acid_raw, org_id=org_id)
            put_text(vroot + "notes.md", f"# {vname}\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        for d in p.get("devices") or []:
            dname = sanitize_name(d)
            droot = f"{root}device/{dname}/"
            _ensure_folders([droot], acid=acid_raw, org_id=org_id)
            put_text(droot + "notes.md", f"# {dname}\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        created["persons"].append(pname)

    # Companies
    for c in companies:
        cname = sanitize_name(c.get("name") or "Unnamed Company")
        root = f"{profiles}company/{cname}/"
        _ensure_folders([root, root + "legal/", root + "financials/", root + "domain/"], acid=acid_raw, org_id=org_id)
        if c.get("notes"):
            put_text(root + "notes.md", f"# {cname}\n\n{c['notes']}\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        for dom in c.get("domains") or []:
            dname = sanitize_name(dom)
            droot = f"{root}domain/{dname}/"
            _ensure_folders([droot], acid=acid_raw, org_id=org_id)
            put_text(droot + "notes.md", f"# {dname}\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
            put_text(droot + "dns-records.txt", "", acid=acid_raw, org_id=org_id, content_type="text/plain")
        created["companies"].append(cname)

    return created


def _seed_defaults_if_empty(tops: dict, created_summary: Dict[str, list], *, acid_raw: str, org_id: Optional[str]) -> None:
    """Create Default Person/Company/Device/Vehicle if nothing was passed."""
    profiles = tops["profiles"]
    if not created_summary["persons"]:
        root = f"{profiles}person/Default Person/"
        _ensure_folders([root, root + "documents/", root + "vehicle/", root + "device/"], acid=acid_raw, org_id=org_id)
        put_text(root + "notes.md", "# Default Person\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        # default sub-profiles
        vroot = f"{root}vehicle/Default Vehicle/"
        droot = f"{root}device/Default Device/"
        _ensure_folders([vroot, droot], acid=acid_raw, org_id=org_id)
        put_text(vroot + "notes.md", "# Default Vehicle\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")
        put_text(droot + "notes.md", "# Default Device\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")

    if not created_summary["companies"]:
        root = f"{profiles}company/Default Company/"
        _ensure_folders([root, root + "legal/", root + "financials/"], acid=acid_raw, org_id=org_id)
        put_text(root + "notes.md", "# Default Company\n", acid=acid_raw, org_id=org_id, content_type="text/markdown")


# ---------- Public API ----------
def provision_account(
    acid_input: str,
    org_id: Optional[str] = None,
    name: Optional[str] = None,
    persons: Optional[List[dict]] = None,
    companies: Optional[List[dict]] = None,
) -> str:
    """
    Create the account scaffold at: a/{acid_display}/...
    Optionally seed profiles from passed arrays; if none passed, create default placeholders.
    Returns the base prefix created.
    """
    acid_raw, acid_disp = normalize_acid(acid_input)
    base = f"a/{acid_disp}/"

    # Build top-level folders & ensure presence
    tops = _folders_top(base, NUMBERED_LAYOUT)
    _ensure_folders(tops.values(), acid=acid_raw, org_id=org_id)

    # Meta files
    now = int(time.time())
    links = {k: f"s3://{S3_BUCKET}/{v}" for k, v in tops.items()}
    account_json = {
        "acid": acid_raw,
        "acid_display": acid_disp,
        "org_id": org_id,
        "name": name,
        "links": links,
        "layout": "numbered" if NUMBERED_LAYOUT else "classic",
        "version": 2,
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

    # v2: seed from payload
    payload = {"persons": persons or [], "companies": companies or []}
    created = _seed_from_payload(tops, payload, acid_raw=acid_raw, org_id=org_id)

    # If nothing was passed, create defaults
    _seed_defaults_if_empty(tops, created, acid_raw=acid_raw, org_id=org_id)

    # Optional legacy demo set
    if SEED_SAMPLE_FILES:
        _seed_sample_files_legacy(tops, acid_raw=acid_raw, acid_disp=acid_disp, org_id=org_id)

    return base
