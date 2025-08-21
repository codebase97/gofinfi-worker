import os
import json
import boto3
from datetime import datetime, timezone

# AWS S3 client setup
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-1"),
)

BUCKET = os.getenv("S3_BUCKET", "gofinfi-portal")
CREATE_KEEP_FILES = os.getenv("CREATE_KEEP_FILES", "true").lower() == "true"
SEED_SAMPLE_FILES = os.getenv("SEED_SAMPLE_FILES", "false").lower() == "true"


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def put_json(key: str, data: dict):
    body = json.dumps(data, indent=2)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")


def put_text(key: str, text: str):
    s3.put_object(Bucket=BUCKET, Key=key, Body=text.encode("utf-8"), ContentType="text/plain")


def put_keep(key: str):
    if CREATE_KEEP_FILES:
        s3.put_object(Bucket=BUCKET, Key=f"{key}.keep", Body=b"", ContentType="text/plain")


def scaffold_account(acid: str, org_id: str, name: str, persons=None, companies=None):
    if persons is None:
        persons = []
    if companies is None:
        companies = []

    acid_display = f"acid-{acid[0:3]}-{acid[3:5]}-{acid[5:]}"
    base = f"a/{acid_display}/"

    # Defaults if no persons/companies passed
    if not persons and not companies:
        persons = [{"name": "Default Person", "notes": "", "devices": ["Default Device"], "vehicles": ["Default Vehicle"]}]
        companies = [{"name": "Default Company", "notes": "", "domains": ["default.com"]}]

    # Core folders
    folders = [
        "_meta/",
        "uploads/",
        "intake/",
        "profiles/",
        "applications/",
        "exports/",
        "shared/",
        "worklog/",
        "archive/",
    ]
    for f in folders:
        put_keep(base + f)

    # profiles.md
    put_text(base + "profiles/profiles.md", "# Profiles Notes\n")

    profiles_summary = {"persons": [], "companies": []}
    relationships = {"people": [], "companies": [], "devices": [], "vehicles": [], "domains": [], "generated_at": iso_now()}

    # Persons
    for person in persons:
        pname = person["name"]
        pbase = f"{base}profiles/person/{pname}/"
        put_keep(pbase)
        put_text(pbase + "notes.md", person.get("notes", ""))

        # Documents
        docs = f"{pbase}documents/"
        put_keep(docs)
        if SEED_SAMPLE_FILES:
            for f in ["passport.pdf", "driver-license.pdf", "utility-bill.pdf", "bank-statement-proof.pdf", "contract.pdf"]:
                put_text(docs + f, f"Sample content for {f}")

        # Vehicles
        for v in person.get("vehicles", []):
            vbase = f"{pbase}vehicle/{v}/"
            put_keep(vbase)
            put_text(vbase + "notes.md", "")
            if SEED_SAMPLE_FILES:
                for f in ["title.pdf", "insurance.pdf"]:
                    put_text(vbase + f, f"Sample content for {f}")
            relationships["vehicles"].append({"name": v, "owner_type": "person", "owner_name": pname, "path": vbase})

        # Devices
        for d in person.get("devices", []):
            dbase = f"{pbase}device/{d}/"
            put_keep(dbase)
            put_text(dbase + "notes.md", "")
            if SEED_SAMPLE_FILES:
                put_text(dbase + "imei.txt", "000000000000000")
            relationships["devices"].append({"name": d, "owner_type": "person", "owner_name": pname, "path": dbase})

        profiles_summary["persons"].append({"name": pname, "path": pbase})
        relationships["people"].append({"name": pname, "path": pbase})

    # Companies
    for company in companies:
        cname = company["name"]
        cbase = f"{base}profiles/company/{cname}/"
        put_keep(cbase)
        put_text(cbase + "notes.md", company.get("notes", ""))

        # Legal
        lbase = f"{cbase}legal/"
        put_keep(lbase)
        if SEED_SAMPLE_FILES:
            for f in ["articles.pdf", "operating-agreement.pdf", "certificate-of-good-standing.pdf",
                      "ein-verification-letter.pdf", "business-license.pdf", "bank-resolution.pdf"]:
                put_text(lbase + f, f"Sample content for {f}")

        # Financials
        fbase = f"{cbase}financials/"
        put_keep(fbase)
        if SEED_SAMPLE_FILES:
            for f in ["monthly-statement-2025-07.pdf", "monthly-statement-2025-06.pdf",
                      "monthly-statement-2025-05.pdf", "tax-return-2023.pdf", "tax-return-2024.pdf"]:
                put_text(fbase + f, f"Sample content for {f}")

        # Domain
        for dom in company.get("domains", []):
            dombase = f"{cbase}domain/{dom}/"
            put_keep(dombase)
            put_text(dombase + "notes.md", "")
            if SEED_SAMPLE_FILES:
                put_text(dombase + "dns-records.txt", "A 127.0.0.1")
            relationships["domains"].append({"name": dom, "owner_type": "company", "owner_name": cname, "path": dombase})

        profiles_summary["companies"].append({"name": cname, "path": cbase})
        relationships["companies"].append({"name": cname, "path": cbase})

    # Meta files
    account_json = {
        "acid": acid,
        "acid_display": acid_display,
        "org_id": org_id,
        "name": name,
        "created_at": iso_now(),
        "layout": "classic",
        "links": {
            "uploads": f"s3://{BUCKET}/{base}uploads/",
            "profiles": f"s3://{BUCKET}/{base}profiles/",
            "applications": f"s3://{BUCKET}/{base}applications/",
            "exports": f"s3://{BUCKET}/{base}exports/"
        },
        "profiles": profiles_summary
    }
    put_json(base + "_meta/account.json", account_json)
    put_json(base + "_meta/relationships.json", relationships)

    return {"status": "ok", "acid": acid_display, "bucket": BUCKET}
