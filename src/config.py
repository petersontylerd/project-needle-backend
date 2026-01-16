"""Application configuration using pydantic-settings.

Loads settings from environment variables with sensible defaults for development.
Supports both local development and Docker deployment via environment overrides.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All settings can be overridden via environment variables or a .env file.
    Docker deployments should override DATABASE_URL (use 'db' hostname) and
    RUNS_ROOT (typically mounted at /data/runs).

    Attributes:
        DATABASE_URL: PostgreSQL connection string with asyncpg driver.
            Local default uses localhost; Docker should use 'db' hostname.
        DEBUG: Enable debug mode with SQL logging.
        RUNS_ROOT: Root directory for Project Needle run outputs.
            Local default uses absolute path; Docker mounts at /data/runs.
        INSIGHT_GRAPH_RUN: Relative path to insight graph run within RUNS_ROOT.
        MODELING_RUN: Relative path to modeling run within RUNS_ROOT.
        CORS_ORIGINS: Allowed origins for CORS (comma-separated).
            Includes localhost for dev and common Docker hostnames.

    Example:
        >>> settings = Settings()
        >>> settings.DATABASE_URL
        'postgresql+asyncpg://postgres:postgres@localhost:5432/quality_compass'

    Docker Example (.env):
        DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/quality_compass
        RUNS_ROOT=/data/runs
        CORS_ORIGINS=http://localhost,http://frontend,http://frontend:80
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database - localhost for local dev, 'db' for Docker
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/quality_compass",
        description="PostgreSQL connection string. Use 'db' hostname in Docker.",
    )

    # Application
    # Security: Default to False - must be explicitly enabled for development
    DEBUG: bool = Field(
        default=False,
        description="Enable debug mode with verbose SQL logging. Set DEBUG=true for development.",
    )
    APP_TITLE: str = Field(
        default="Quality Compass API",
        description="Application title shown in OpenAPI docs.",
    )
    APP_VERSION: str = Field(
        default="0.1.0",
        description="API version string.",
    )

    # Project Needle paths - Docker mount point default, override for local development
    RUNS_ROOT: str = Field(
        default="/data/runs",
        description="Root directory for Project Needle outputs. Set via RUNS_ROOT environment variable for local dev.",
    )
    INSIGHT_GRAPH_RUN: str = Field(
        default="_unset",
        description="Relative path to insight graph run within RUNS_ROOT (e.g., test_minimal/20260101120000). Set explicitly in .env for local dev.",
    )
    MODELING_RUN: str = Field(
        default="modeling/lightgbm_optauto/groupby=_global_group/group=all/estimator=lightgbm",
        description="Relative path to modeling run within RUNS_ROOT.",
    )

    # CORS - includes common Docker hostnames
    CORS_ORIGINS: str = Field(
        default="http://localhost:4200,http://127.0.0.1:4200,http://localhost,http://frontend,http://frontend:4200",
        description="Comma-separated allowed CORS origins. Includes Docker container names.",
    )

    # dbt Documentation
    DBT_DOCS_URL: str = Field(
        default="http://localhost:8080",
        description="Base URL for dbt documentation server. Used for deep-linking to model docs.",
    )
    DBT_PROJECT_PATH: str = Field(
        default="",
        description="Path to dbt project directory. If empty, defaults to backend/dbt relative to project root.",
    )

    # Taxonomy (semantic layer source of truth)
    TAXONOMY_PATH: str = Field(
        default="/app/taxonomy",
        description="Path to taxonomy directory containing metrics.yaml, edge_types.yaml. Docker default is /app/taxonomy.",
    )

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS_ORIGINS into a list.

        Returns:
            list[str]: List of allowed origins.
        """
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]


settings = Settings()
