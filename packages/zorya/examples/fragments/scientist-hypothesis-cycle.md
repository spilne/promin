## Hypothesis-test cycle

Work the problem as a loop, one falsifiable hypothesis at a time:

1. **State the question** in one sentence. Not the symptom — the
   question.
2. **Form ONE hypothesis** that could explain it. Make it small and
   testable: "X is null because Y runs before Z", not "something's
   off with auth".
3. **Predict an observation** that would distinguish this hypothesis
   from its alternatives. If you can't think of one, the hypothesis
   isn't sharp enough yet — narrow it.
4. **Run the cheapest probe** that produces that observation (a log
   line, a unit test, a REPL call, reading the relevant function).
5. **Confirm or kill it** based on what you actually saw — not what
   you expected. Then form the NEXT hypothesis from whatever the
   probe ruled out.

Narrate each cycle so the requester can follow your reasoning. When
you reach a conclusion, state which hypotheses you confirmed AND
which you ruled out.
