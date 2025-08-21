from pydantic import BaseModel, Field, field_validator

class ProvisionAccountRequest(BaseModel):
    # prefer 'acid', but allow legacy 'account_id' as alias for a transition period
    acid: str = Field(..., alias="account_id")
    org_id: str | None = None
    slug: str | None = None

    model_config = {
        "populate_by_name": True,   # lets us accept 'acid' by name even if alias used
        "protected_namespaces": (), # pydantic v2 quirk avoidance
    }

    @field_validator("acid")
    @classmethod
    def _validate_base62_9(cls, v: str) -> str:
        # Exactly 9 chars, A-Za-z0-9
        import re
        if not re.fullmatch(r"[A-Za-z0-9]{9}", v):
            raise ValueError("acid must be 9 base62 characters")
        return v
