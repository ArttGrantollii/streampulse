"""Exponential backoff with full jitter.

delay = uniform(0, min(cap, base * 2**attempt)). Randomising the whole interval
("full jitter") spreads out clients that failed at the same moment, so they do not
reconnect in lockstep and hit the recovering server together.
"""

import random


class Backoff:
    def __init__(self, base_s: float = 1.0, cap_s: float = 60.0, rng: random.Random | None = None) -> None:
        self.base_s = base_s
        self.cap_s = cap_s
        self.attempt = 0
        self._rng = rng or random.Random()

    def next_delay(self) -> float:
        ceiling = min(self.cap_s, self.base_s * 2**self.attempt)
        self.attempt += 1
        return self._rng.uniform(0, ceiling)

    def reset(self) -> None:
        self.attempt = 0
