# Backend Architecture

This document describes how the Quality Compass backend functions, including data flow, component responsibilities, and integration points.

## System Overview

The backend is a FastAPI application that serves as the API layer between the Project Needle analytics engine and the Angular frontend (Quality Compass dashboard). It consumes processed analytics data, transforms it through dbt, stores it in PostgreSQL, and exposes it via REST APIs.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Project Needle Ecosystem                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐    │
│  │  project-needle  │     │ project-needle-  │     │ project-needle-  │    │
│  │ (Analytics Engine)│────▶│    backend       │────▶│      web         │    │
│  │                  │     │  (This Repo)     │     │ (Angular UI)     │    │
│  └──────────────────┘     └──────────────────┘     └──────────────────┘    │
│         │                         │                                         │
│         │ writes                  │ reads                                   │
│         ▼                         ▼                                         │
│  ┌──────────────────┐     ┌──────────────────┐                             │
│  │ needle-artifacts │     │   PostgreSQL     │                             │
│  │ (Docker Volume)  │     │   Database       │                             │
│  └──────────────────┘     └──────────────────┘                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Pipeline

The backend processes data through several stages:

### Stage 1: Artifact Ingestion

The analytics engine (project-needle) writes JSON artifacts to a shared Docker volume (`needle-artifacts`). These contain:
- Node results from the insight graph
- Temporal analysis data
- Modeling experiment results
- SHAP feature importance values

### Stage 2: Raw Table Loading

The script `scripts/load_insight_graph_to_dbt.py` reads artifacts and loads them into PostgreSQL raw tables:
- `raw_node_results` - Insight graph node data
- `raw_modeling_runs` - ML experiment metadata
- `raw_modeling_experiments` - Individual experiment results

### Stage 3: dbt Transformation

dbt transforms raw data through three layers:

```
raw_* tables
    │
    ▼
┌─────────────────────────────────────────┐
│              STAGING LAYER              │
│  (stg_* models - data cleaning)         │
│  - stg_node_results                     │
│  - stg_modeling_experiments             │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│           INTERMEDIATE LAYER            │
│  (int_* models - aggregations/joins)    │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│              MARTS LAYER                │
│  (fct_*, dim_* models - analytics)      │
│  - fct_signals (primary output)         │
│  - fct_contributions                    │
│  - fct_model_drivers                    │
│  - dim_facilities                       │
│  - dim_metrics                          │
└─────────────────────────────────────────┘
```

Output tables are created in the `public_marts` schema.

### Stage 4: Signal Hydration

The `SignalHydrator` service reads from `public_marts.fct_signals` and upserts records into the application's `signals` table. This separation allows:

- dbt to handle data transformation and quality checks
- The signals table to maintain workflow state (assignments, activity events)
- Clean boundary between analytics data and application data

Hydration runs:
1. Automatically on application startup (if signals table is empty)
2. On-demand via CLI: `python -m src.signals.cli hydrate`

### Stage 5: API Serving

FastAPI endpoints read from the application tables and return JSON responses to the frontend.

## Application Layers

### Router Layer (`src/{module}/router.py`)

Each API module has a router that:
- Defines HTTP endpoints with path parameters and query filters
- Validates request data using Pydantic schemas
- Delegates business logic to services
- Formats responses

Routers are registered in `src/main.py` under the `/api` prefix.

### Schema Layer (`src/schemas/`)

Pydantic models for:
- Request validation (body parameters)
- Response serialization
- Data transfer between layers

### Service Layer (`src/services/`)

Business logic services:
- `SignalHydrator` - Bridges dbt marts to application tables
- `ContributionService` - Queries contribution analysis from dbt
- `NarrativeService` - Parses markdown narratives from artifacts
- `DbtMetadataService` - Reads dbt manifest/catalog for metadata endpoints

### Database Layer (`src/db/`)

SQLAlchemy 2.0 async models and session management:
- `models.py` - ORM models (Signal, User, Assignment, ActivityEvent)
- `session.py` - Async engine and session factory
- `base.py` - Declarative base class

## Request Flow

```
HTTP Request
    │
    ▼
┌─────────────────────────────────────────┐
│            FastAPI Router               │
│  - Path parameter extraction            │
│  - Query parameter validation           │
│  - Authentication (future)              │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│         Dependency Injection            │
│  - Database session (DbSession)         │
│  - Service instances                    │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│            Route Handler                │
│  - Request schema validation            │
│  - Business logic (via services)        │
│  - Database queries                     │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│          Response Formation             │
│  - ORM to Pydantic conversion           │
│  - JSON serialization                   │
└─────────────────────────────────────────┘
    │
    ▼
HTTP Response
```

## Key Design Patterns

### Dependency Injection

Database sessions are injected via FastAPI's `Depends`:

```python
from src.dependencies import DbSession

@router.get("/signals")
async def list_signals(session: DbSession) -> SignalListResponse:
    # session is automatically provided and managed
    ...
```

### Async/Await Throughout

All database operations use SQLAlchemy's async API with `asyncpg`:
- `async_sessionmaker` creates sessions
- `await session.execute(query)` runs queries
- Auto-commit on success, rollback on exception

### Bulk Operations

The `SignalHydrator` uses PostgreSQL's `INSERT...ON CONFLICT` for efficient upserts:
- Batches up to 1000 records per statement
- Single round-trip per batch instead of per-record
- Handles both inserts and updates atomically

### Type Annotations

Extensive use of Python 3.12+ type hints:
- Pydantic models for runtime validation
- SQLAlchemy `Mapped[T]` for column types
- `Annotated` for dependency injection types

## Configuration

Settings are loaded from environment variables via `pydantic-settings`:

```python
from src.config import settings

# Access configuration
db_url = settings.DATABASE_URL
runs_root = settings.RUNS_ROOT
```

Key settings:
- `DATABASE_URL` - PostgreSQL connection string
- `RUNS_ROOT` - Path to insight-graph artifacts
- `INSIGHT_GRAPH_RUN` - Specific run to process
- `TAXONOMY_PATH` - Path to taxonomy submodule
- `CORS_ORIGINS` - Allowed frontend origins

## Startup Behavior

On application start (`src/main.py`):

1. FastAPI app is created with lifespan context manager
2. Database connection pool is established
3. Signal count is checked:
   - If empty: `SignalHydrator.hydrate_signals()` runs
   - If populated: hydration is skipped
4. Routes are registered
5. Server begins accepting requests

## Error Handling

- HTTP exceptions raised in routes return appropriate status codes
- Database exceptions trigger automatic session rollback
- Service errors are caught and converted to HTTP 500 responses
- All errors are logged with context
