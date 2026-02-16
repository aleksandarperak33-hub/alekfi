from __future__ import annotations

from alekfi.api.app import create_app


def test_institutional_health_and_metrics_routes_are_registered() -> None:
    app = create_app()
    paths = {route.path for route in app.router.routes}
    assert "/api/agentos/health" in paths
    assert "/api/metrics/signal_quality" in paths
    assert "/api/metrics/forecast_calibration" in paths
