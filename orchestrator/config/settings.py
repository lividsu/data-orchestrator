from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_url: str = "sqlite:///orchestrator.db"
    timezone: str = "Asia/Shanghai"
    ui_port: int = 8501
    ui_host: str = "0.0.0.0"
    log_level: str = "INFO"
    log_dir: str = "./logs"
    default_retry_times: int = 3
    default_retry_delay: float = 5.0
    default_retry_backoff: float = 2.0
    default_timeout_seconds: float = 60.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ORCHESTRATOR_",
    )
