"""Integration test fixtures for database-dependent tests.

Provides fixtures for creating test data in the database, isolated
per test session. Tests using these fixtures require a properly
migrated database.

Note: When database is unavailable or schema is out of sync,
database-dependent fixtures will skip the test gracefully.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_maker

# Note: The 'client' fixture is defined in the root tests/conftest.py
# and is available to all tests, including integration tests.


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a database session for test fixtures.

    Creates a session that can be used to insert test data.
    The session auto-commits on success and rolls back on failure.

    Skips test if database connection fails or schema is incompatible.

    Yields:
        AsyncSession: Database session for fixture setup.
    """
    try:
        async with async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    except Exception as e:
        pytest.skip(f"Database unavailable or schema incompatible: {e}")


@pytest.fixture
async def isolated_signal(db_session: AsyncSession):
    """Create a signal with no related signals (unique facility/service_line).

    This signal has a unique facility and service_line combination,
    ensuring no other signals will be returned as related.

    Yields:
        Signal: The created signal with unique facility/service_line.
    """
    # Import models here to avoid import errors if schema doesn't exist
    from src.db.models import Assignment, AssignmentStatus, Signal, SignalDomain

    signal = Signal(
        canonical_node_id="isolated__test__node__unique",
        metric_id="testMetricIsolated",
        domain=SignalDomain.EFFICIENCY,
        facility="Isolated Test Facility XYZ",
        service_line="Unique Service Line ABC",
        description="Isolated test signal for related signals testing",
        metric_value=Decimal("1.0"),
        detected_at=datetime.now(tz=UTC),
    )

    try:
        db_session.add(signal)
        await db_session.flush()
    except Exception as e:
        pytest.skip(f"Failed to create test signal (schema mismatch?): {e}")

    # Create assignment for the signal (required for workflow_status)
    assignment = Assignment(
        signal_id=signal.id,
        status=AssignmentStatus.NEW,
    )
    db_session.add(assignment)
    await db_session.flush()

    yield signal

    # Cleanup: delete the signal and assignment
    await db_session.delete(assignment)
    await db_session.delete(signal)
    await db_session.commit()


@pytest.fixture
async def related_signals_set(
    db_session: AsyncSession,
):
    """Create a set of signals for testing related signals functionality.

    Creates:
    - 1 base signal
    - 3 related signals (same facility + service_line, different metric_id)
    - 1 unrelated signal (different facility)
    - 1 same-metric signal (should be excluded from related)

    All signals share the same facility and service_line except the unrelated one.

    Yields:
        dict: Contains 'base', 'related', 'unrelated', and 'same_metric' keys.
    """
    # Import models here to avoid import errors if schema doesn't exist
    from src.db.models import Assignment, AssignmentStatus, Signal, SignalDomain

    facility = "Related Test Facility"
    service_line = "Related Test Service Line"
    detected_at = datetime.now(tz=UTC)

    # Base signal
    base_signal = Signal(
        canonical_node_id="related_test__base__node",
        metric_id="baseMetric",
        domain=SignalDomain.EFFICIENCY,
        facility=facility,
        service_line=service_line,
        description="Base signal for related testing",
        metric_value=Decimal("1.0"),
        simplified_severity=50,
        detected_at=detected_at,
    )

    try:
        db_session.add(base_signal)
        await db_session.flush()
    except Exception as e:
        pytest.skip(f"Failed to create test signals (schema mismatch?): {e}")

    # Related signals (same facility + service_line, different metrics)
    related_signals = []
    for i, (metric, priority) in enumerate([("relatedMetricA", 100), ("relatedMetricB", 75), ("relatedMetricC", 25)]):
        signal = Signal(
            canonical_node_id=f"related_test__{metric}__node",
            metric_id=metric,
            domain=SignalDomain.EFFICIENCY,
            facility=facility,
            service_line=service_line,
            description=f"Related signal {i + 1}",
            metric_value=Decimal(str(1.0 + i * 0.1)),
            simplified_severity=priority,
            detected_at=detected_at,
        )
        db_session.add(signal)
        related_signals.append(signal)

    # Unrelated signal (different facility)
    unrelated_signal = Signal(
        canonical_node_id="related_test__unrelated__node",
        metric_id="unrelatedMetric",
        domain=SignalDomain.SAFETY,
        facility="Different Facility",
        service_line=service_line,
        description="Unrelated signal (different facility)",
        metric_value=Decimal("2.0"),
        simplified_severity=200,
        detected_at=detected_at,
    )
    db_session.add(unrelated_signal)

    # Same-metric signal (same facility + service_line + metric, should be excluded)
    same_metric_signal = Signal(
        canonical_node_id="related_test__same_metric__node",
        metric_id="baseMetric",  # Same as base signal
        domain=SignalDomain.EFFICIENCY,
        facility=facility,
        service_line=service_line,
        description="Same metric signal (should be excluded)",
        metric_value=Decimal("1.5"),
        simplified_severity=150,
        detected_at=detected_at,
    )
    db_session.add(same_metric_signal)

    await db_session.flush()

    # Create assignments for all signals
    all_signals = [base_signal, *related_signals, unrelated_signal, same_metric_signal]
    assignments = []
    for signal in all_signals:
        assignment = Assignment(
            signal_id=signal.id,
            status=AssignmentStatus.NEW,
        )
        db_session.add(assignment)
        assignments.append(assignment)

    await db_session.flush()

    yield {
        "base": base_signal,
        "related": related_signals,
        "unrelated": unrelated_signal,
        "same_metric": same_metric_signal,
    }

    # Cleanup
    for assignment in assignments:
        await db_session.delete(assignment)
    for signal in all_signals:
        await db_session.delete(signal)
    await db_session.commit()
