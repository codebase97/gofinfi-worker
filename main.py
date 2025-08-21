import os
from typing import Optional, List

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aws import provision_account, fmt_acid_display

app = FastAPI()

PROVISIONER_SECRET = os.getenv("PROVISIONER_SECRET")


class PersonIn(BaseModel):
    name: str
    notes: Optional[str] = None
    devices: Optional[List[str]] = None
    vehicles: Optional[List[str]] = None


class CompanyIn(BaseModel):
    name: str
    notes: Optional[str] = None
    domains: Optional[List[str]] = None


class ProvisionAccountRequest(BaseModel):
    # Accept either raw 9-char or display 'acid-XXX-XX-XXXX'
    acid: str
    org_id: Optional[str] = None
    name: Optional[str] = None
    persons: Optional[List[PersonIn]] = None
    companies: Optional[List[CompanyIn]] = None


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "provisioner", "env": os.getenv("RAILWAY_ENVIRONMENT", "local")}


@app.post("/provision-account")
def provision_account_endpoint(
    payload: ProvisionAccountRequest,
    x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret"),
):
    # Simple shared-secret check (Railway var: PROVISIONER_SECRET)
    if PROVISIONER_SECRET and x_internal_secret != PROVISIONER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        base_prefix = provision_account(
            acid_input=payload.acid,
            org_id=payload.org_id,
            name=payload.name,
            persons=[p.dict() for p in (payload.persons or [])],
            companies=[c.dict() for c in (payload.companies or [])],
        )
        return JSONResponse(
            {
                "ok": True,
                "acid": payload.acid,
                "acid_display": fmt_acid_display(payload.acid),
                "prefix": base_prefix,
                "profiles_seeded": {
                    "persons_passed": len(payload.persons or []),
                    "companies_passed": len(payload.companies or []),
                    "defaults_used": (len(payload.persons or []) == 0 and len(payload.companies or []) == 0),
                },
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provision failed: {e}")
