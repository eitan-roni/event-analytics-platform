from typing import Any

from fastapi import APIRouter, Depends, Request, Response, status

from app.services.readiness_service import ReadinessService

router = APIRouter(tags=["operations"])


def get_readiness_service(request: Request) -> ReadinessService:
    return request.app.state.readiness_service


@router.get("/health")
def health_check() -> dict[str, str]:
    """Liveness: the process is up and serving. No dependency calls."""
    return {"status": "ok"}


@router.get("/ready")
def readiness_check(
    response: Response, service: ReadinessService = Depends(get_readiness_service)
) -> dict[str, Any]:
    """Readiness: 503 unless every critical dependency (PostgreSQL, Kafka) is up.

    Redis is reported but not critical, because the stats cache fails open.
    """
    report = service.check()
    if not report.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    checks: dict[str, dict[str, Any]] = {}
    for dependency in report.dependencies:
        entry: dict[str, Any] = {
            "status": "up" if dependency.up else "down",
            "critical": dependency.critical,
        }
        if dependency.error is not None:
            entry["error"] = dependency.error
        checks[dependency.name] = entry
    return {"status": "ready" if report.ready else "not_ready", "checks": checks}
