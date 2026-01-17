# API Reference

This document describes the REST API endpoints exposed by the Quality Compass backend.

Base URL: `http://localhost:8000`

Interactive documentation: `http://localhost:8000/docs` (Swagger UI)

## Health Check

### GET /health

Returns API health status.

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

---

## Signals API

Endpoints for quality signals detected by the analytics engine.

### GET /api/signals

List signals with filtering, sorting, and pagination.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| domain | string | Filter by quality domain (Efficiency, Safety, Effectiveness) |
| facility | string | Filter by facility name(s), comma-separated for multiple |
| system_name | string | Filter by health system name |
| service_line | string | Filter by service line |
| status | string | Filter by assignment status (new, assigned, in_progress, resolved, closed) |
| signal_type | string | Filter by simplified signal type |
| sort_by | string | Sort field: detected_at, priority, metric_id (default: detected_at) |
| sort_order | string | Sort order: asc, desc (default: desc) |
| limit | integer | Results per page, 1-100 (default: 25) |
| offset | integer | Results to skip (default: 0) |

**Response:**
```json
{
  "total_count": 150,
  "offset": 0,
  "limit": 25,
  "signals": [
    {
      "id": "uuid",
      "canonical_node_id": "losIndex__medicareId__aggregate",
      "metric_id": "losIndex",
      "domain": "Efficiency",
      "facility": "General Hospital",
      "facility_id": "010033",
      "system_name": "ALPHA_HEALTH",
      "service_line": "Cardiology",
      "description": "LOS Index above benchmark",
      "metric_value": 1.25,
      "peer_mean": 1.0,
      "percentile_rank": 85.5,
      "encounters": 450,
      "detected_at": "2025-12-15T10:00:00Z",
      "days_open": 5,
      "simplified_signal_type": "chronic_underperformer",
      "simplified_severity": 75,
      "workflow_status": "new",
      "has_children": true,
      "has_parent": false
    }
  ]
}
```

### GET /api/signals/{signal_id}

Get a single signal by UUID.

**Response:** Single signal object (same structure as list item)

### PATCH /api/signals/{signal_id}

Update signal fields (description, why_matters_narrative).

**Request Body:**
```json
{
  "description": "Updated description",
  "why_matters_narrative": "Business impact explanation"
}
```

### GET /api/signals/{signal_id}/temporal

Get temporal trend data for a signal.

**Response:**
```json
{
  "signal_id": "uuid",
  "temporal_node_id": "losIndex__medicareId__dischargeMonth",
  "slope_percentile": 25.5,
  "monthly_z_scores": [-0.2, -0.4, -0.5],
  "has_temporal_data": true
}
```

### GET /api/signals/{signal_id}/contributions

Get hierarchical contribution analysis.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| top_n | integer | Number of top contributors (default: 10, max: 50) |

**Response:**
```json
{
  "upward_contribution": {
    "entity_name": "Cardiology",
    "contribution_pct": 35.2,
    "excess_value": 0.15
  },
  "downward_contributions": [
    {
      "entity_name": "Heart Failure",
      "contribution_pct": 22.5,
      "excess_value": 0.08
    }
  ],
  "signal_hierarchy_level": "service_line",
  "has_children": true,
  "has_parent": true
}
```

### GET /api/signals/{signal_id}/technical-details

Get detailed statistical information.

**Response:**
```json
{
  "simple_zscore": 1.25,
  "robust_zscore": 1.18,
  "percentile_rank": 85.5,
  "peer_std": 0.15,
  "peer_count": 120,
  "slope": 0.02,
  "slope_percentile": 72.0,
  "magnitude_tier": "elevated",
  "trajectory_tier": "deteriorating",
  "consistency_tier": "persistent",
  "simplified_signal_type": "chronic_underperformer",
  "simplified_severity": 75,
  "simplified_reasoning": "High percentile rank with deteriorating trend"
}
```

### GET /api/signals/{signal_id}/related

Get signals with same facility and service line but different metrics.

### GET /api/signals/{signal_id}/children

Get child signals via drills_to edges.

### GET /api/signals/{signal_id}/parent

Get parent signal via drills_to edge.

### GET /api/signals/filter-options

Get distinct values for filter dropdowns.

**Response:**
```json
{
  "metric_id": ["losIndex", "readmissionRate", "mortalityIndex"],
  "domain": ["Efficiency", "Safety", "Effectiveness"],
  "simplified_signal_type": ["critical_trajectory", "chronic_underperformer"],
  "system_name": ["ALPHA_HEALTH", "BETA_MEDICAL"],
  "service_line": ["Cardiology", "Orthopedics"]
}
```

### GET /api/signals/facilities

Get distinct facility names.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| system_name | string | Filter by health system |

---

## Workflow API

Signal assignment and status management.

### POST /api/signals/{signal_id}/assign

Assign a signal to a user.

**Request Body:**
```json
{
  "assignee_id": "user-uuid",
  "role_type": "clinical_leadership",
  "notes": "Please review by end of week"
}
```

**Response:**
```json
{
  "id": "assignment-uuid",
  "signal_id": "signal-uuid",
  "assignee_id": "user-uuid",
  "status": "assigned",
  "role_type": "clinical_leadership",
  "assigned_at": "2025-12-15T10:00:00Z"
}
```

### PATCH /api/signals/{signal_id}/status

Update workflow status with validated transitions.

**Valid Transitions:**
- new → assigned
- assigned → in_progress, new
- in_progress → resolved, assigned
- resolved → closed, in_progress
- closed → (terminal state)

**Request Body:**
```json
{
  "status": "in_progress",
  "notes": "Starting investigation"
}
```

### GET /api/signals/{signal_id}/assignment

Get current assignment for a signal.

---

## Activity Feed API

Activity events and audit trail.

### GET /api/feed

Get activity feed with cursor-based pagination.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| signal_id | uuid | Filter by signal |
| event_type | string | Filter by type (assignment, status_change, etc.) |
| limit | integer | Events per page (default: 20, max: 100) |
| cursor | string | ISO datetime for pagination |

**Response:**
```json
{
  "events": [
    {
      "id": "event-uuid",
      "event_type": "status_change",
      "signal_id": "signal-uuid",
      "user_id": "user-uuid",
      "payload": {
        "old_status": "assigned",
        "new_status": "in_progress"
      },
      "read": false,
      "created_at": "2025-12-15T10:00:00Z"
    }
  ],
  "cursor": "2025-12-15T09:00:00Z",
  "has_more": true,
  "total_count": 150
}
```

### GET /api/feed/unread-count

Get count of unread events.

### POST /api/feed/{event_id}/mark-read

Mark an event as read.

---

## Narratives API

Markdown narrative analysis for facilities.

### GET /api/narratives

List facilities with available narratives.

**Response:**
```json
{
  "facilities": ["AFP658", "KYR088", "UMC001"],
  "count": 3
}
```

### GET /api/narratives/{facility_id}

Get full narrative insights.

**Response:**
```json
{
  "facility_id": "AFP658",
  "metric_value": 1.25,
  "generated_at": "2025-12-15T10:00:00Z",
  "executive_summary": {
    "pareto_insight": "Top 3 segments account for 65% of excess",
    "top_contributors_higher": [...],
    "top_contributors_lower": [...]
  },
  "pareto_analysis": {...},
  "top_drivers": {...},
  "hierarchical_breakdown": [...]
}
```

### GET /api/narratives/{facility_id}/summary

Get lightweight executive summary (faster response).

---

## Metadata API

dbt metadata and semantic layer information.

### GET /api/metadata/summary

Get dbt project summary.

**Response:**
```json
{
  "project_name": "quality_compass",
  "dbt_version": "1.9.1",
  "model_count": 15,
  "source_count": 3,
  "metric_count": 8,
  "docs_base_url": "http://localhost:8080"
}
```

### GET /api/metadata/models

List all dbt models with optional filtering.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| layer | string | Filter by layer (staging, marts, semantic) |
| tag | string | Filter by tag |

### GET /api/metadata/models/{model_name}

Get specific model metadata.

### GET /api/metadata/models/{model_name}/lineage

Get model lineage (upstream and downstream dependencies).

### GET /api/metadata/bundle

Get semantic metadata bundle from taxonomy.

**Response:**
```json
{
  "version": "1.0.0",
  "generated_at": "2025-12-15T10:00:00Z",
  "taxonomy_hash": "abc123...",
  "metrics": [...],
  "edge_types": [...],
  "tag_types": [...],
  "comparison_modes": [...],
  "group_by_types": [...],
  "group_by_sets": [...]
}
```

### POST /api/metadata/refresh

Force refresh of cached dbt artifacts.

---

## Modeling API

ML model performance and feature drivers.

### GET /api/modeling/summary

Get modeling run summary.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| run_id | string | Filter by run ID |

**Response:**
```json
{
  "run": {
    "run_id": "20251215100000",
    "status": "completed",
    "groups": ["_global_group"],
    "duration_seconds": 3600
  },
  "experiments": [...],
  "experiment_count": 5,
  "best_experiment": {...}
}
```

### GET /api/modeling/experiments

List all modeling experiments.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| run_id | string | Filter by run ID |
| estimator | string | Filter by estimator (ridge, xgboost, lightgbm) |
| limit | integer | Max results (default: 100) |

### GET /api/modeling/drivers

Get top feature drivers with SHAP values.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| run_id | string | Filter by run ID |
| estimator | string | Filter by estimator |
| top_n | integer | Number of drivers (default: 10, max: 50) |

**Response:**
```json
{
  "run_id": "20251215100000",
  "estimator": "lightgbm",
  "drivers": [
    {
      "feature_name": "age",
      "shap_value": 0.25,
      "direction": "positive",
      "rank": 1
    }
  ],
  "top_n": 10
}
```

---

## Users API

User management endpoints.

### GET /api/users

List all users.

### GET /api/users/{user_id}

Get user by ID.

### POST /api/users

Create a new user.

---

## Runs API

Analytics run management.

### GET /api/runs

List available insight graph runs.

### GET /api/runs/{run_id}

Get run details.

---

## Ontology API

Taxonomy and knowledge graph endpoints.

### GET /api/ontology/metrics

Get metric definitions from taxonomy.

### GET /api/ontology/dimensions

Get dimension definitions.

---

## Error Responses

All endpoints return standard error responses:

**400 Bad Request:**
```json
{
  "detail": "Invalid status transition from 'new' to 'in_progress'"
}
```

**404 Not Found:**
```json
{
  "detail": "Signal not found: uuid"
}
```

**500 Internal Server Error:**
```json
{
  "detail": "Database connection failed"
}
```
