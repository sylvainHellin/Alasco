"""Generic batch runner that collects per-item success/failure results.

Property-agnostic. Lets callers run a callable over a list of items and get a
structured per-item outcome instead of failing the whole batch on first error.
"""

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class ItemResult[T, R]:
    """Outcome of processing a single item."""

    item: T
    ok: bool
    result: R | None = None
    error: str | None = None


@dataclass
class BatchResult[T, R]:
    """Aggregate outcome of a batch run."""

    results: list[ItemResult[T, R]] = field(default_factory=list)

    @property
    def succeeded(self) -> list[ItemResult[T, R]]:
        return [r for r in self.results if r.ok]

    @property
    def failed(self) -> list[ItemResult[T, R]]:
        return [r for r in self.results if not r.ok]

    @property
    def all_ok(self) -> bool:
        return all(r.ok for r in self.results)


def batch[T, R](
    items: list[T],
    func: Callable[[T], R],
    *,
    stop_on_error: bool = False,
) -> BatchResult[T, R]:
    """Run ``func`` for each item, capturing per-item success or failure.

    Args:
        items: Items to process.
        func: Callable applied to each item.
        stop_on_error: If True, stop after the first failure (default False:
            process every item and report partial failures).

    Returns:
        A BatchResult with one ItemResult per processed item.
    """
    batch_result: BatchResult[T, R] = BatchResult()
    for item in items:
        try:
            result = func(item)
            batch_result.results.append(ItemResult(item=item, ok=True, result=result))
        except Exception as exc:  # noqa: BLE001 - per-item isolation is intentional
            batch_result.results.append(ItemResult(item=item, ok=False, error=str(exc)))
            if stop_on_error:
                break
    return batch_result
