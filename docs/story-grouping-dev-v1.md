# Story grouping guide — development v1

Decide which headlines report the same story. Group only from what the worksheet shows and the
linked page; do not use sentiment labels or model output.

**Same story:** the headlines report the same specific report, announcement or occurrence. A
rewrite, a follow-up copy of the same news, or a repeat of the same link all count.

**Not the same story:** a shared company, topic, market or date alone is not enough. Two earnings
reports from one company in different quarters are different stories. So are two different jobs
reports published in the same week.

**Uncertain:** you cannot tell from the available text. Use `uncertain:` and say why. Do not guess,
and do not mark something `alone` just because it's unclear.

Write one line per decision. Every headline number appears exactly once:

```text
story: 1 7 | Same central bank rate decision
uncertain: 3 9 | Both about a merger; can't tell if it is the same deal
alone: 2 4 5 6 8
```

`compare:` hints on the worksheet are computed from links and shared words. They only suggest what
to read side by side. Ignore them when they are wrong, and look for matches they missed.

`alone` means you found no related headline in this cohort. It does not prove none exists elsewhere.
