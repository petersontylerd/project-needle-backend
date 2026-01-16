"""Quality Compass Backend API - FastAPI application entry point.

This module creates and configures the FastAPI application with:
- CORS middleware for frontend access
- Health check endpoint
- API router mounting
- Startup hydration of signals from Project Needle data (only if DB is empty)
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select

from src.config import settings
from src.db.models import Signal
from src.db.session import async_session_maker
from src.services.signal_hydrator import SignalHydrator

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application lifespan events.

    On startup:
    - Hydrates signals from Project Needle node result files into the database
      (only if the signals table is empty)

    Args:
        app: FastAPI application instance.

    Yields:
        None: After startup tasks complete.
    """
    # Startup: check if hydration is needed
    try:
        async with async_session_maker() as session:
            signal_count = await session.scalar(select(func.count()).select_from(Signal))

        if signal_count and signal_count > 0:
            logger.info(
                "Skipping hydration: %d signals already exist in database",
                signal_count,
            )
        else:
            logger.info("Starting signal hydration from Project Needle data...")
            hydrator = SignalHydrator()
            stats = await hydrator.hydrate_signals()
            logger.info(
                "Signal hydration complete: %d files, %d signals created",
                stats["files_processed"],
                stats["signals_created"],
            )
    except Exception as e:
        logger.error("Signal hydration failed: %s", e)
        # Don't fail startup - the API can still work without hydrated data

    yield

    # Shutdown: cleanup if needed
    logger.info("Application shutting down")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        FastAPI: Configured application instance with middleware and routes.

    Example:
        >>> app = create_app()
        >>> # Run with: uvicorn src.main:app --reload
    """
    app = FastAPI(
        lifespan=lifespan,
        title=settings.APP_TITLE,
        version=settings.APP_VERSION,
        description="Healthcare analytics dashboard API for signal triage and quality improvement tracking.",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Configure CORS for frontend access
    # Security: Use explicit methods/headers instead of wildcards
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["content-type", "authorization", "accept"],
    )

    # Register routes
    _register_routes(app)

    return app


def _register_routes(app: FastAPI) -> None:
    """Register API routes on the application.

    Args:
        app: FastAPI application instance.

    Note:
        Routers are added as they are implemented in Tasks 3.I, 3.J, 3.K.
    """
    # Import routers
    from src.activity import router as activity_router
    from src.metadata import router as metadata_router
    from src.metrics import router as metrics_router
    from src.modeling import router as modeling_router
    from src.narratives import router as narratives_router
    from src.ontology.router import router as ontology_router
    from src.runs import router as runs_router
    from src.signals import router as signals_router
    from src.users import router as users_router
    from src.workflow import router as workflow_router

    # Health check endpoint at root
    @app.get("/health", tags=["health"])
    async def health_check() -> dict[str, str]:
        """Check API health status.

        Returns:
            dict: Health status with API version.
        """
        return {"status": "healthy", "version": settings.APP_VERSION}

    # API routes
    app.include_router(signals_router, prefix="/api")
    app.include_router(workflow_router, prefix="/api")
    app.include_router(activity_router, prefix="/api")
    app.include_router(narratives_router, prefix="/api")
    app.include_router(metrics_router, prefix="/api")
    app.include_router(metadata_router, prefix="/api")
    app.include_router(modeling_router, prefix="/api")
    app.include_router(ontology_router, prefix="/api")
    app.include_router(runs_router, prefix="/api")
    app.include_router(users_router, prefix="/api")


# Create the application instance
app = create_app()
