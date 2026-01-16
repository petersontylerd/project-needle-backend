"""Alembic environment configuration for async SQLAlchemy.

Configures Alembic to work with async PostgreSQL connections
and the Quality Compass database models.
"""

import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Import models for autogenerate support
from src.config import settings
from src.db.base import Base
from src.db.models import ActivityEvent, Assignment, Signal, User  # noqa: F401

# Alembic Config object
config = context.config

# Set the database URL from settings
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# Configure Python logging from config file
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Model MetaData for 'autogenerate' support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    Configures the context with just a URL and not an Engine.
    Useful for generating SQL scripts without database connectivity.

    Calls to context.execute() emit the given string to the
    script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
        version_num_width=128,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Execute migrations with the given connection.

    Args:
        connection: SQLAlchemy database connection.
    """
    from sqlalchemy import text

    # Ensure alembic_version table has sufficient column width for long revision IDs.
    # Default Alembic uses VARCHAR(32), but our revision IDs like
    # "010_consolidate_signal_schema" exceed that limit.
    #
    # This handles both cases:
    # 1. New database: Creates table with VARCHAR(128)
    # 2. Existing database: Alters column to VARCHAR(128) if needed
    connection.execute(
        text(
            """
            DO $$
            BEGIN
                -- Create table if it doesn't exist
                CREATE TABLE IF NOT EXISTS alembic_version (
                    version_num VARCHAR(128) NOT NULL,
                    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
                );

                -- Alter column width if table exists with smaller column
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'alembic_version'
                    AND column_name = 'version_num'
                    AND character_maximum_length < 128
                ) THEN
                    ALTER TABLE alembic_version
                    ALTER COLUMN version_num TYPE VARCHAR(128);
                END IF;
            END $$;
            """
        )
    )
    connection.commit()

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,
        version_num_width=128,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations in 'online' mode with async engine.

    Creates an async Engine and associates a connection with the context.
    """
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    Uses async event loop to execute migrations.
    """
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
