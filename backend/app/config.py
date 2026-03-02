"""Application configuration from environment."""

from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings loaded from environment (and .env)."""

    model_config = SettingsConfigDict(env_prefix="HOUSES_", extra="ignore")

    # DynamoDB (default local; set to "" for real AWS)
    dynamodb_endpoint_url: Optional[str] = "http://localhost:8000"
    dynamodb_region: str = "eu-west-2"
    table_house_price_performance: str = "house_price_performance"
    table_street_performance: str = "street_performance"
    table_property_sales: str = "property_sales"
    table_dimension_index: str = "dimension_index"


settings = Settings()
