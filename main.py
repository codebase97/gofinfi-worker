import os
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
import uvicorn
from aws import scaffold_account

app = FastAPI()

PROVISIONER_SECRET = os.getenv("PROVISIONER_SECRET", "changeme")


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "worker", "env": os.getenv("ENV", "development")}


@app.post("/create-account")
async def create_account(request: Request, x_internal_secret: str = Header(None)):
    if x_internal_secret != PROVISIONER_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    body = await request.json()
    acid = body.get("acid")
    org_id = body.get("org_id")
    name = body.get("name")
    persons = body.get("persons", [])
    companies = body.get("companies", [])

    if not acid or not org_id or not name:
        return JSONResponse(status_code=400, content={"error": "Missing required fields"})

    result = scaffold_account(acid, org_id, name, persons, companies)
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 3000)))
