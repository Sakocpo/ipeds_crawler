from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    headless: bool = True
    timeout_ms: int = 15_000
    min_year: int = 2014
    max_year: int = 2023
    concurrency: int = 3
    user_agent: str = "ipeds-crawler/0.1"
    out_csv: str = "data/processed/ipeds.csv"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_prefix="IPEDS_", env_file=".env", extra="ignore")
