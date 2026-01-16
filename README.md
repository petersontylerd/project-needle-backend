# Quality Compass Backend

FastAPI backend for the Quality Compass healthcare analytics dashboard.

## Multi-Repository Ecosystem

This repository is part of the Project Needle ecosystem:

| Repository | Description | Tech Stack |
|------------|-------------|------------|
| [project-needle](https://github.com/petersontylerd/project-needle) | Analytics Engine | Python 3.14, uv |
| **project-needle-backend** (this repo) | FastAPI backend + dbt | Python 3.12, PostgreSQL |
| [project-needle-web](https://github.com/petersontylerd/project-needle-web) | Angular dashboard | Angular 21, Nx, pnpm |

For complete multi-repo setup, see [project-needle/docs/repo-setup.md](https://github.com/petersontylerd/project-needle/blob/main/docs/repo-setup.md).

## Overview

The backend provides REST APIs for:
- **Signal Management** - CRUD operations for quality signals with filtering, sorting, and pagination
- **Workflow** - Signal assignment, status transitions (New/In Progress/Completed)
- **Activity Feed** - Audit trail of signal state changes
- **Narratives** - Markdown report parsing and structured data extraction
- **Metrics** - Metric definitions and aggregations via semantic layer
- **Metadata** - Semantic layer endpoints for dimensions and metrics
- **Modeling** - ML model artifacts and predictions

## Project Structure

```
project-needle-backend/
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
│   └── metadata/            # Semantic layer endpoints
├── dbt/                     # dbt transformation layer
├── alembic/                 # Database migrations
├── taxonomy/                # Shared vocabularies (git submodule)
├── tests/                   # Backend test suite
├── scripts/                 # Data loading and utility scripts
└── docker/                  # Docker configuration
```

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16+
- uv package manager
- Docker (for containerized setup)

### Docker Setup (Recommended)

The backend uses a shared Docker volume (`needle-artifacts`) to receive artifacts from the analytics engine.

```bash
# One-time setup: create shared volume
docker volume create needle-artifacts

# Copy environment file
cp .env.example .env

# Build and start services
docker compose build
docker compose up -d

# Run database migrations
docker exec project-needle-backend-api alembic upgrade head
```

Services available at:
- **API**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **PostgreSQL**: localhost:5433

### Local Development

```bash
# Install dependencies
uv sync --extra dev

# Run migrations
uv run alembic upgrade head

# Start development server
uv run uvicorn src.main:app --reload --port 8000
```

## Data Pipeline

The backend consumes artifacts from the analytics engine (project-needle) via the shared Docker volume.

### Pipeline Flow

```
project-needle (Analytics Engine)
    │ writes artifacts to
    ▼
needle-artifacts (Docker Volume)
    │ read by
    ▼
load_insight_graph_to_dbt.py → raw tables
    │
    ▼
dbt run → staging → marts (fct_signals, dim_*)
    │
    ▼
signals.cli hydrate → signals table
    │
    ▼
FastAPI → REST API → Angular Frontend
```

### Loading Data

After running insight-graph in project-needle:

```bash
# List available runs
docker run --rm -v needle-artifacts:/data alpine ls /data/

# Load artifacts into database
docker exec project-needle-backend-api python /app/scripts/load_insight_graph_to_dbt.py \
  --runs-root /data/runs \
  --insight-graph-run test_minimal/<TIMESTAMP> \
  --database-url "postgresql://postgres:postgres@db:5432/quality_compass"

# Run dbt transformations
docker exec project-needle-backend-api bash -c "cd /app/dbt && dbt deps && dbt run"

# Hydrate signals table
docker exec project-needle-backend-api python -m src.signals.cli hydrate
```

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

## Development

### Running Tests

```bash
uv run pytest tests/ -v
```

### Code Quality

```bash
# Format
uv run ruff format src/ tests/

# Lint
uv run ruff check src/ tests/

# Type check
uv run mypy src/
```

### Creating Migrations

```bash
uv run alembic revision -m "description_of_change"
```

### Updating Taxonomy Submodule

The taxonomy is shared with project-needle via git submodule:

```bash
git submodule update --remote taxonomy
git add taxonomy
git commit -m "chore: update taxonomy submodule"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection string |
| `RUNS_ROOT` | `/data/runs` | Path to insight-graph artifacts |
| `CORS_ORIGINS` | `http://localhost:4200` | Allowed CORS origins |
| `DEBUG` | `false` | Enable debug mode |
| `TAXONOMY_PATH` | `/app/taxonomy` | Path to taxonomy files |

## Docker Compose Reference

```yaml
services:
  db:
    image: quality-compass-db:dev
    ports:
      - "5433:5432"

  backend:
    image: quality-compass-backend:dev
    volumes:
      - needle-artifacts:/data/runs:ro  # Shared volume (read-only)
    ports:
      - "8000:8000"
    depends_on:
      - db

volumes:
  needle-artifacts:
    external: true
```

## Related Documentation

- [Multi-Repo Setup Guide](https://github.com/petersontylerd/project-needle/blob/main/docs/repo-setup.md)
- [API Reference](https://github.com/petersontylerd/project-needle/blob/main/docs/api-reference.md)
- [dbt Models](dbt/README.md)
