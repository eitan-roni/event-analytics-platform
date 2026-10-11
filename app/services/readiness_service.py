from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass

MAX_ERROR_LENGTH = 200
# Upper bound for the whole check. Probes have their own short timeouts, but
# some waits are outside their control (e.g. DNS resolution of a host whose
# container is gone can take several seconds); a probe still running at the
# deadline is reported as down.
DEFAULT_DEADLINE_SECONDS = 3.0


@dataclass(frozen=True)
class DependencyCheck:
    """One dependency probe; the probe raises if the dependency is unavailable."""

    name: str
    critical: bool
    probe: Callable[[], None]


@dataclass(frozen=True)
class DependencyStatus:
    name: str
    critical: bool
    up: bool
    error: str | None = None


@dataclass(frozen=True)
class ReadinessReport:
    dependencies: list[DependencyStatus]

    @property
    def ready(self) -> bool:
        """Ready when every critical dependency is up; others are only reported."""
        return all(status.up for status in self.dependencies if status.critical)


def _run(check: DependencyCheck) -> DependencyStatus:
    try:
        check.probe()
    except Exception as exc:  # any failure means "down"; the reason is reported
        error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
        return DependencyStatus(check.name, check.critical, up=False, error=error)
    return DependencyStatus(check.name, check.critical, up=True)


class ReadinessService:
    def __init__(
        self, checks: list[DependencyCheck], deadline_seconds: float = DEFAULT_DEADLINE_SECONDS
    ) -> None:
        self._checks = checks
        self._deadline_seconds = deadline_seconds

    def check(self) -> ReadinessReport:
        # In parallel, so the slowest probe (capped by the deadline) bounds
        # the response time.
        executor = ThreadPoolExecutor(max_workers=max(len(self._checks), 1))
        try:
            futures = [executor.submit(_run, check) for check in self._checks]
            wait(futures, timeout=self._deadline_seconds)
        finally:
            # Do not wait for probes that overran; they finish in the background.
            executor.shutdown(wait=False, cancel_futures=True)

        statuses = []
        for check, future in zip(self._checks, futures, strict=True):
            if future.done():
                statuses.append(future.result())
            else:
                error = f"timed out after {self._deadline_seconds}s"
                statuses.append(DependencyStatus(check.name, check.critical, up=False, error=error))
        return ReadinessReport(statuses)
