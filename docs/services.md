# Services

This document describes the service layer components that contain business logic.

## Overview

Services encapsulate business logic and data access patterns that are reused across multiple endpoints. They are located in `src/services/` and are injected into route handlers via FastAPI's dependency injection.

## SignalHydrator

**Location:** `src/services/signal_hydrator.py`

The SignalHydrator bridges the dbt transformation layer with the application's signals table. It queries the `fct_signals` dbt mart and upserts records into the signals table.

### Purpose

- Read transformed signal data from dbt output (`public_marts.fct_signals`)
- Map dbt columns to SQLAlchemy model fields
- Perform bulk upserts using PostgreSQL's `INSERT...ON CONFLICT`
- Fetch on-demand technical details for drill-down views

### Usage

**Automatic startup hydration:**
```python
# In src/main.py lifespan context
hydrator = SignalHydrator()
stats = await hydrator.hydrate_signals()
```

**Manual hydration via CLI:**
```bash
python -m src.signals.cli hydrate
```

**With filters:**
```python
hydrator = SignalHydrator(
    run_id="20251210170210",
    facility_ids=["010033", "010045"],
    limit=1000,
)
stats = await hydrator.hydrate_signals()
```

### Key Methods

**`hydrate_signals()`**
- Queries all signals from `fct_signals`
- Processes in batches of 1000 records
- Returns statistics: signals_processed, signals_created, signals_updated, signals_skipped

**`get_technical_details(canonical_node_id, entity_dimensions_hash)`**
- Fetches detailed z-scores and classification data for a single signal
- Used by the `/technical-details` endpoint
- Returns statistical methods, anomaly labels, and tiers

**`get_signal_count()` / `get_fct_signal_count()`**
- Compare source (dbt) vs destination (app) signal counts

### Bulk Upsert Strategy

The hydrator uses a single multi-row INSERT statement per batch:

```python
insert_stmt = insert(Signal.__table__).values(records)
upsert_stmt = insert_stmt.on_conflict_do_update(
    constraint="uq_signals_entity_metric_detected",
    set_={
        "metric_value": insert_stmt.excluded.metric_value,
        # ... other updatable fields
    },
)
await session.execute(upsert_stmt)
```

This reduces database round-trips from N to 1 per batch.

---

## ContributionService

**Location:** `src/services/contribution_service.py`

Provides hierarchical contribution analysis by querying the `fct_contributions` dbt mart.

### Purpose

- Calculate upward contribution (how this entity contributes to its parent)
- Calculate downward contributions (how children contribute to this entity)
- Support drill-down navigation in the dashboard

### Usage

```python
from src.services.contribution_service import ContributionService

service = ContributionService()
upward, downward, hierarchy_level = await service.get_hierarchical_contributions(
    signal=signal,
    top_n=10,
)
```

### Key Methods

**`get_hierarchical_contributions(signal, top_n)`**
- Returns tuple of (upward_contribution, downward_contributions, hierarchy_level)
- Queries `public_marts.fct_contributions` with facility and metric filters
- Determines hierarchy level from entity_dimensions

**`to_response(contribution_row)`**
- Converts database row to response schema

### Error Handling

```python
class ContributionServiceError(Exception):
    def __init__(self, message: str):
        self.message = message
```

Raised when:
- Database query fails
- Required data is missing
- Contribution table doesn't exist

---

## NarrativeService

**Location:** `src/services/narrative_service.py`

Parses markdown narrative reports from analytics artifacts and extracts structured data.

### Purpose

- List facilities with available narrative reports
- Parse markdown into structured sections
- Extract executive summaries, Pareto analysis, and hierarchical breakdowns

### Usage

```python
from src.services.narrative_service import NarrativeService

service = NarrativeService()
facilities = service.list_available_facilities()
insights = service.get_narrative("AFP658")
```

### Key Methods

**`list_available_facilities()`**
- Scans `RUNS_ROOT/INSIGHT_GRAPH_RUN/narratives/` directory
- Returns list of facility IDs with `.md` files

**`get_narrative(facility_id)`**
- Reads and parses markdown file for facility
- Returns `NarrativeInsights` dataclass with structured data

### Data Structures

```python
@dataclass
class NarrativeInsights:
    facility_id: str
    metric_value: float
    generated_at: str
    executive_summary: ExecutiveSummary
    cross_metric_comparison: list[CrossMetricEntry]
    pareto_analysis: ParetoAnalysis
    top_drivers: TopDrivers
    insights: InsightCategories
    hierarchical_breakdown: list[HierarchyNode]
```

### Error Handling

```python
class NarrativeServiceError(Exception):
    def __init__(self, message: str):
        self.message = message
```

Raised when:
- Markdown file not found
- Parsing fails
- Required sections missing

---

## DbtMetadataService

**Location:** `src/services/dbt_metadata_service.py`

Reads dbt artifacts (manifest.json, catalog.json) to expose model metadata and lineage.

### Purpose

- Parse dbt manifest for model definitions
- Parse dbt catalog for column types
- Generate documentation URLs
- Provide lineage graph data

### Usage

```python
from src.services.dbt_metadata_service import get_dbt_metadata_service

service = get_dbt_metadata_service()
summary = service.get_summary()
models = service.get_all_models()
lineage = service.get_lineage("fct_signals")
```

### Key Methods

**`get_summary()`**
- Returns project name, dbt version, model/source/metric counts
- Reads from `dbt/target/manifest.json`

**`get_all_models()`**
- Returns list of all model metadata
- Includes columns, tags, dependencies

**`get_model(model_name)`**
- Returns metadata for specific model

**`get_lineage(model_name)`**
- Returns upstream and downstream dependencies
- Traverses the dependency graph

**`get_docs_url(resource_type, name)`**
- Generates deep-link URL to dbt docs server

**`refresh_cache()`**
- Clears cached artifacts for re-reading

### Caching

The service caches parsed artifacts in memory. Use `refresh_cache()` after running `dbt docs generate` to pick up changes.

---

## Dependency Injection Pattern

Services are injected into routes using FastAPI's `Depends`:

```python
from fastapi import Depends
from typing import Annotated

def get_narrative_service() -> NarrativeService:
    return NarrativeService()

NarrativeServiceDep = Annotated[NarrativeService, Depends(get_narrative_service)]

@router.get("/{facility_id}")
async def get_narrative(
    facility_id: str,
    service: NarrativeServiceDep,
) -> NarrativeInsightsResponse:
    insights = service.get_narrative(facility_id)
    ...
```

This pattern enables:
- Consistent service instantiation
- Easy mocking for tests
- Clear dependency declaration

---

## Adding New Services

1. Create service class in `src/services/`
2. Define dependency injection function
3. Create type alias with `Annotated`
4. Inject into route handlers
5. Add unit tests in `tests/services/`

Example template:

```python
# src/services/my_service.py

class MyService:
    def __init__(self, setting: str = None):
        self.setting = setting or settings.MY_SETTING

    async def do_something(self, input: str) -> dict:
        # Business logic here
        return {"result": input}


class MyServiceError(Exception):
    def __init__(self, message: str):
        self.message = message


# Dependency injection
def get_my_service() -> MyService:
    return MyService()
```

```python
# In router
from src.services.my_service import MyService, get_my_service

MyServiceDep = Annotated[MyService, Depends(get_my_service)]

@router.get("/endpoint")
async def endpoint(service: MyServiceDep):
    return await service.do_something("input")
```
