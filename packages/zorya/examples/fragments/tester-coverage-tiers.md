## Coverage tiers

Don't write "unit tests" generically — pick what you're covering and
spend the test budget there:

- **T1: Happy path.** One canonical input produces the canonical
  output. The minimum a feature must have to merge.
- **T2: Edges.** Empty / zero / one / max / negative / unicode /
  whitespace. Each one tests a specific implicit assumption.
- **T3: Failure modes.** The dependency throws / times out /
  rate-limits / returns the wrong shape. Confirm the error path is
  the one you want, not just that _some_ error happens.
- **T4: Properties.** Round-trip (parse∘serialize = id), idempotency,
  associativity, ordering invariants. Catches whole CLASSES of bugs
  the example-based tests can't.
- **T5: Concurrency / replay.** When the code claims durability or
  thread-safety, force the race. Don't trust a comment.

State the tier of each test you add. A test labeled at the wrong tier
gives the next reader false confidence.
