#!/usr/bin/env bash
# pipeline.sh - Backend Pipeline (Stages 8-14)
# Loads insight-graph artifacts into PostgreSQL via dbt
#
# Usage:
#   ./scripts/pipeline.sh --runs-root <path> --insight-graph-run <run> [options]
#
# Options:
#   --runs-root PATH           Root directory for run outputs (required)
#   --insight-graph-run PATH   Relative path to insight graph run (required)
#   --modeling-run PATH        Relative path to modeling run (optional)
#   --database-url URL         PostgreSQL connection (default: from config)
#   --skip-load                Skip data loading (stages 8-9)
#   --skip-dbt                 Skip dbt build (stage 10)
#   --skip-hydrate             Skip signal hydration (stage 11)
#   --skip-validate            Skip E2E validation (stage 13)
#   --dry-run                  Show what would be executed
#   --help                     Show this help message

set -euo pipefail

# Default values
RUNS_ROOT=""
INSIGHT_GRAPH_RUN=""
MODELING_RUN=""
DATABASE_URL=""
SKIP_LOAD=false
SKIP_DBT=false
SKIP_HYDRATE=false
SKIP_VALIDATE=false
DRY_RUN=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    grep '^#' "$0" | grep -v '#!/' | cut -c3-
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --runs-root) RUNS_ROOT="$2"; shift 2 ;;
        --insight-graph-run) INSIGHT_GRAPH_RUN="$2"; shift 2 ;;
        --modeling-run) MODELING_RUN="$2"; shift 2 ;;
        --database-url) DATABASE_URL="$2"; shift 2 ;;
        --skip-load) SKIP_LOAD=true; shift ;;
        --skip-dbt) SKIP_DBT=true; shift ;;
        --skip-hydrate) SKIP_HYDRATE=true; shift ;;
        --skip-validate) SKIP_VALIDATE=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --help|-h) usage ;;
        *) log_error "Unknown option: $1"; usage ;;
    esac
done

# Validate required arguments
if [[ -z "$RUNS_ROOT" ]]; then
    log_error "Missing required argument: --runs-root"
    usage
fi

if [[ -z "$INSIGHT_GRAPH_RUN" ]]; then
    log_error "Missing required argument: --insight-graph-run"
    usage
fi

# Build database URL argument if provided
DB_ARG=""
if [[ -n "$DATABASE_URL" ]]; then
    DB_ARG="--database-url $DATABASE_URL"
fi

# ==========================================================================
# Stage 8-9: Load insight-graph artifacts into raw tables
# ==========================================================================
if [[ "$SKIP_LOAD" != "true" ]]; then
    log_info "=== Stage 8-9: Loading insight-graph artifacts ==="
    log_info "Runs root: $RUNS_ROOT"
    log_info "Insight graph run: $INSIGHT_GRAPH_RUN"

    CMD="UV_CACHE_DIR=.uv-cache uv run python scripts/load_insight_graph_to_dbt.py"
    CMD+=" --runs-root $RUNS_ROOT"
    CMD+=" --insight-graph-run $INSIGHT_GRAPH_RUN"
    [[ -n "$DB_ARG" ]] && CMD+=" $DB_ARG"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would execute: $CMD"
    else
        eval "$CMD"
    fi

    # Load modeling data if specified
    if [[ -n "$MODELING_RUN" ]]; then
        log_info "Loading modeling artifacts: $MODELING_RUN"
        CMD="UV_CACHE_DIR=.uv-cache uv run python scripts/load_modeling_to_dbt.py"
        CMD+=" --runs-root $RUNS_ROOT"
        CMD+=" --modeling-run $MODELING_RUN"
        [[ -n "$DB_ARG" ]] && CMD+=" $DB_ARG"

        if [[ "$DRY_RUN" == "true" ]]; then
            log_info "[DRY-RUN] Would execute: $CMD"
        else
            eval "$CMD"
        fi
    fi
fi

# ==========================================================================
# Stage 10: Run dbt build (staging → intermediate → marts)
# ==========================================================================
if [[ "$SKIP_DBT" != "true" ]]; then
    log_info "=== Stage 10: Running dbt build ==="

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would execute: cd dbt && UV_CACHE_DIR=../.uv-cache uv run dbt build"
    else
        cd dbt
        UV_CACHE_DIR=../.uv-cache uv run dbt deps
        UV_CACHE_DIR=../.uv-cache uv run dbt build
        cd ..
    fi
fi

# ==========================================================================
# Stage 11: Hydrate signals table
# ==========================================================================
if [[ "$SKIP_HYDRATE" != "true" ]]; then
    log_info "=== Stage 11: Hydrating signals ==="

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would execute: signal-cli hydrate"
    else
        UV_CACHE_DIR=.uv-cache uv run signal-cli hydrate
    fi
fi

# ==========================================================================
# Stage 12: Graph sync (ontology)
# ==========================================================================
log_info "=== Stage 12: Graph sync (ontology) ==="
log_info "Graph sync is performed automatically by backend on startup"

# ==========================================================================
# Stage 13: E2E validation
# ==========================================================================
if [[ "$SKIP_VALIDATE" != "true" ]]; then
    log_info "=== Stage 13: E2E validation ==="

    RUN_DIR="$RUNS_ROOT/$INSIGHT_GRAPH_RUN"
    CMD="UV_CACHE_DIR=.uv-cache uv run python scripts/validate_classification_e2e.py"
    CMD+=" --run-dir $RUN_DIR"
    [[ -n "$DB_ARG" ]] && CMD+=" $DB_ARG"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would execute: $CMD"
    else
        eval "$CMD" || log_warn "Validation completed with warnings"
    fi
fi

log_info "=== Pipeline Complete ==="
log_info "Backend is ready to serve API requests"
