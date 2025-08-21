import os
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aws import scaffold_account
from api_v201 import router as v201_router  # v2.0.1 endpoints

PROVISIONER_SECRET = os.getenv("PROVISIONER_SECRET")

app = FastAPI()
app.include_router(v201_router)


class Person(BaseModel):
    name: str
    notes: Optional[str] = ""
    devices: Optional[List[str]] = []
    vehicles: Optional[List[str]] = []


class Company(BaseModel):
    name: str
    notes: Optional[str] = ""
    domains: Optional[List[str]] = []


class ProvisionAccountRequest(BaseModel):
    acid: str
    org_id: Optional[str] = None
    name: Optional[str] = None
    persons: Optional[List[Person]] = []
    companies: Optional[List[Company]] = []


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "provisioner", "env": os.getenv("RAILWAY_ENVIRONMENT", "local")}


@app.post("/create-account")
def create_account_endpoint(
    payload: ProvisionAccountRequest,
    x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret"),
):
    if PROVISIONER_SECRET and x_internal_secret != PROVISIONER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        result = scaffold_account(
            acid=payload.acid,
            org_id=payload.org_id,
            name=payload.name,
            persons=[p.dict() for p in (payload.persons or [])],
            companies=[c.dict() for c in (payload.companies or [])],
        )
        return JSONResponse({"ok": True, **result})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provision failed: {e}")