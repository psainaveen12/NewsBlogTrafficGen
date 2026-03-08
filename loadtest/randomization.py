from __future__ import annotations

import random
from typing import Callable, Iterable, TypeVar

T = TypeVar("T")
def weighted_choice(items: Iterable[T], weight_fn: Callable[[T], float]) -> T:
    candidates: list[T] = []
    weights: list[float] = []
    for item in items:
        weight = float(weight_fn(item))
        if weight > 0:
            candidates.append(item)
            weights.append(weight)

    if not candidates:
        raise ValueError("Cannot choose from empty sequence")
    return random.choices(candidates, weights=weights, k=1)[0]
