"""Drop redelivered events.

Delivery is at-least-once: redelivered copies share an `event_id`, and in the sample some
carry a timestamp re-stamped by 1-5 seconds. `event_id` is therefore the identity of an event
and the first copy seen wins, which is what a streaming dedupe does without having to retract
anything already emitted. Ties are broken by arrival order, so a replay of the same input
produces the same output.

In Beam this is a stateful DoFn keyed by `event_id` with a TTL timer (or the built-in
`Deduplicate` transform); here the state is an in-memory set.
"""

from __future__ import annotations

from event_metrics.models import CleanEvent


class FirstSeenDeduplicator:
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self.duplicate_count = 0

    def accept(self, event: CleanEvent) -> bool:
        """True the first time an `event_id` is seen, False for every later copy."""
        if event.event_id in self._seen:
            self.duplicate_count += 1
            return False
        self._seen.add(event.event_id)
        return True
