import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aws import provision_account, fmt_acid_display

app = FastAPI()

PROVISIONER_SECRET = os.getenv("PROVISIONER_SECRET")


class ProvisionAccountRequest(BaseModel):
    acid: str
    org_id: Optional[str] = None
    slug: Optional[str] = None  # reserved for future use


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "provisioner", "env": os.getenv("RAILWAY_ENVIRONMENT", "local")}


@app.post("/provision-account")
def provision_account_endpoint(
    payload: ProvisionAccountRequest,
    x_internal_secret: Optional[str] = Header(default=None, alias="x-internal-secret"),
):
    if PROVISIONER_SECRET and x_internal_secret != PROVISIONER_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        prefix = provision_account(payload.acid, payload.org_id)
        return JSONResponse(
            {
                "ok": True,
                "acid": payload.acid,
                "acid_display": fmt_acid_display(payload.acid),
                "prefix": prefix,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provision failed: {e}")
