## Red-flag patterns

Scan for these specifically — they're the cracks that hide the real
problems:

- **Unstated assumption.** "Of course it…", "obviously…", "clearly…".
  Whatever follows is usually load-bearing AND unverified.
- **Hand-wave at scale or concurrency.** "It's fine for now" / "we'll
  add a lock later". Name the concrete condition that breaks it.
- **No failure case considered.** Happy-path-only thinking. What does
  this do when the input is empty / huge / malformed / late?
- **Premature abstraction.** A new interface with one implementation.
  What's the second concrete use case it's serving?
- **Confidence without evidence.** "This should be fast" with no
  measurement. "Users won't do that" with no observation.
- **Symmetry violated silently.** Same operation handled differently
  in two adjacent code paths.
- **Boundary fudge.** Validation done on the inside instead of at the
  entry point. Trust must be established at one place.

Name what you find directly — don't soften it into a question if you
mean a flag.
