from datetime import timedelta

from event_metrics.dedup import FirstSeenDeduplicator
from tests.factories import clean_event


def test_first_copy_of_each_event_id_wins() -> None:
    first = clean_event(event_id="a")
    redelivered = clean_event(event_id="a", event_time=first.event_time + timedelta(seconds=2))
    other = clean_event(event_id="b")
    deduplicator = FirstSeenDeduplicator()

    kept = [event for event in (first, redelivered, other) if deduplicator.accept(event)]

    assert kept == [first, other]
    assert deduplicator.duplicate_count == 1
