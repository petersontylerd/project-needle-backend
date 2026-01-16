# Quality Compass Backend

FastAPI backend for the Quality Compass healthcare analytics dashboard.

## Overview

The backend provides REST APIs for:
- **Signal Management** - CRUD operations for quality signals with filtering, sorting, and pagination
- **Workflow** - Signal assignment, status transitions (New/In Progress/Completed)
- **Activity Feed** - Audit trail of signal state changes
- **Narratives** - Markdown report parsing and structured data extraction
- **Metrics** - Metric definitions and aggregations via semantic layer
- **Metadata** - Semantic layer endpoints for dimensions and metrics
- **Modeling** - ML model artifacts and predictions
- **Ontology** - Domain taxonomy and classification endpoints
- **Runs** - Insight graph run management
- **Users** - User management and preferences

## Project Structure

```
backend/
├── src/
│   ├── main.py              # FastAPI application entry point
│   ├── config.py            # Settings and environment configuration
│   ├── dependencies.py      # Dependency injection
│   ├── db/
│   │   ├── models.py        # SQLAlchemy ORM models
│   │   └── session.py       # Database session management
│   ├── schemas/             # Pydantic request/response schemas
│   ├── services/            # Business logic (SignalHydrator, etc.)
│   ├── signals/             # Signal CRUD, filtering, detail views
│   ├── workflow/            # Assignment, status transitions
│   ├── activity/            # Activity feed with pagination
│   ├── narratives/          # Narrative generation
│   ├── metrics/             # Metrics endpoints
│   ├── metadata/            # Semantic layer endpoints
│   ├── modeling/            # ML data endpoints
│   ├── ontology/            # Domain taxonomy
│   ├── runs/                # Run management
│   └── users/               # User management
├── dbt/                     # dbt transformation layer (see dbt/README.md)
├── alembic/                 # Database migrations
├── tests/                   # Backend test suite
├── scripts/                 # Data loading and utility scripts
└── logs/                    # Application logs
```

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16+
- uv package manager

### Installation

```bash
cd backend
UV_CACHE_DIR=../.uv-cache uv sync --extra dev
```

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/quality_compass

# CORS (comma-separated origins)
CORS_ORIGINS=http://localhost:4200,http://localhost:4000

# Application
APP_TITLE="Quality Compass API"
APP_VERSION="1.0.0"

# Project Needle integration
RUNS_ROOT=/path/to/runs
```

### Database Setup

```bash
# Run migrations
cd backend
UV_CACHE_DIR=../.uv-cache uv run alembic upgrade head

# Load initial data via dbt
cd dbt
UV_CACHE_DIR=../../.uv-cache uv run dbt run
```

### Running the Server

```bash
cd backend
UV_CACHE_DIR=../.uv-cache uv run uvicorn src.main:app --reload --port 8000
```

The API will be available at:
- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Health Check

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | API health status |

### Signals (`/api/signals`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/signals` | GET | List signals with filtering, sorting, pagination |
| `/api/signals/{id}` | GET | Get signal details |
| `/api/signals/{id}` | PATCH | Update signal (status, assignment, notes) |
| `/api/signals/{id}/contributions` | GET | Get contribution analysis |
| `/api/signals/{id}/children` | GET | Get child signals (hierarchical navigation) |
| `/api/signals/{id}/parent` | GET | Get parent signal |

### Workflow (`/api/workflow`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/workflow/signals/{id}/assign` | POST | Assign signal to user |
| `/api/workflow/signals/{id}/status` | PATCH | Update signal status |

### Activity (`/api/activity`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/activity` | GET | Get activity feed with pagination |
| `/api/activity/signals/{id}` | GET | Get activity for specific signal |

### Metadata (`/api/metadata`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metadata/dimensions` | GET | List available dimensions |
| `/api/metadata/metrics` | GET | List available metrics |

### Metrics (`/api/metrics`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metrics/query` | POST | Query metrics via semantic layer |

## Development

### Running Tests

```bash
cd backend
UV_CACHE_DIR=../.uv-cache uv run pytest tests/ -v
```

### Running Specific Tests

```bash
# Single test file
UV_CACHE_DIR=../.uv-cache uv run pytest tests/signals/test_router.py -v

# Single test
UV_CACHE_DIR=../.uv-cache uv run pytest tests/signals/test_router.py::test_list_signals -v
```

### Code Quality

```bash
# Format
UV_CACHE_DIR=../.uv-cache uv run ruff format src/ tests/

# Lint
UV_CACHE_DIR=../.uv-cache uv run ruff check src/ tests/

# Type check
UV_CACHE_DIR=../.uv-cache uv run mypy src/
```

### Creating Migrations

```bash
cd backend
UV_CACHE_DIR=../.uv-cache uv run alembic revision -m "description_of_change"
```

## Data Pipeline

The backend relies on dbt for data transformation. See [dbt/README.md](dbt/README.md) for:
- dbt model documentation
- Data loading procedures
- Semantic layer configuration

### Data Flow

```
Project Needle Run Output (JSONL)
    ↓ Python ETL (scripts/load_insight_graph_to_dbt.py)
Raw Tables (raw_node_results, raw_contributions)
    ↓ dbt run
Staging → Intermediate → Marts (fct_signals, dim_*)
    ↓ FastAPI
REST API → Angular Frontend
```

## Architecture Notes

### Signal Hydration

On startup, the backend checks if the signals table is empty. If so, it hydrates signals from Project Needle node result files using `SignalHydrator`. This is a one-time operation; subsequent runs skip hydration.

### CORS Configuration

CORS is configured to allow the Angular frontend to access the API. Origins are specified via the `CORS_ORIGINS` environment variable.

### Database Sessions

The backend uses async SQLAlchemy with asyncpg for PostgreSQL. Sessions are managed via dependency injection.

## Related Documentation

- [dbt Project](dbt/README.md) - Data transformation layer
- [Docker Setup](../docs/docker-setup.md) - Container deployment
- [API Reference](../docs/api-reference.md) - Detailed API documentation
- [E2E Testing](../docs/e2e.md) - End-to-end test documentation
