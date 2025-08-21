from pydantic_settings import BaseSettings
from pydantic import Field
class Settings(BaseSettings):
    aws_region: str = Field(alias="AWS_REGION", default="us-east-1")
    aws_access_key_id: str | None = Field(alias="AWS_ACCESS_KEY_ID", default=None)
    aws_secret_access_key: str | None = Field(alias="AWS_SECRET_ACCESS_KEY", default=None)
    s3_bucket: str = Field(alias="S3_BUCKET", default="gofinfi-portal")
    provisioner_secret: str = Field(alias="PROVISIONER_SECRET", default="supersecret")
    s3_kms_key_id: str | None = Field(default=None, alias="S3_KMS_KEY_ID")
    numbered_layout: bool = Field(default=False, alias="NUMBERED_LAYOUT")
    aliases_enabled: bool = Field(default=True, alias="ALIASES_ENABLED")
    class Config:
        env_file = ".env"
        extra = "ignore"
settings = Settings()
