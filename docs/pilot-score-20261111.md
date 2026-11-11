# Pilot score table (development; not a finding about the model)

Model revision `0bc2ac10dda6929bcb1f77ccc43c7ec5c424f031`; classes positive, negative, neutral.

| Counts | |
| --- | ---: |
| snapshot versions | 41 |
| eligible versions | 34 |
| labeled | 26 |
| unresolved | 8 |
| labeled assisted | 15 |
| with prediction | 34 |
| in story cohort | 34 |
| uncertain story membership | 0 |
| scored all labels | 26 |
| scored unassisted only | 11 |
| labeled without prediction | 0 |

## all scorable labels — 26 scorable versions, 26 of 34 stories with a scorable anchor

| Block | Condition | Total weight | Accuracy | Macro-F1 | Macro-F1 95% interval |
| --- | --- | ---: | ---: | ---: | --- |
| paired | A | 26.00 | 0.769 | 0.752 | 0.562 to 0.909 |
| paired | B | 26.00 | 0.769 | 0.752 | 0.562 to 0.909 |
| paired | C | 26.00 | 0.769 | 0.752 | 0.562 to 0.909 |
| unrestricted | A | 26.00 | 0.769 | 0.752 | 0.562 to 0.909 |
| unrestricted | B | 26.00 | 0.769 | 0.752 | 0.562 to 0.909 |

## unassisted labels only — 11 scorable versions, 11 of 34 stories with a scorable anchor

| Block | Condition | Total weight | Accuracy | Macro-F1 | Macro-F1 95% interval |
| --- | --- | ---: | ---: | ---: | --- |
| paired | A | 11.00 | 0.636 | 0.600 | 0.278 to 0.895 |
| paired | B | 11.00 | 0.636 | 0.600 | 0.278 to 0.895 |
| paired | C | 11.00 | 0.636 | 0.600 | 0.278 to 0.895 |
| unrestricted | A | 11.00 | 0.636 | 0.600 | 0.278 to 0.895 |
| unrestricted | B | 11.00 | 0.636 | 0.600 | 0.278 to 0.895 |

## Limitations

- Development pilot on a small cohort; the numbers describe whether the method runs, not the model.
- One annotator, who also assigned the story groups; not adjudicated multi-annotator ground truth.
- Labels were released before these predictions. The prediction tool never opened them (tested), but the order was not enforced by time as it must be in the final study.
- Metric conventions (undefined values, macro-F1 averaging, interval method) are development choices to be frozen before any final test.
- When no story has several scorable versions, conditions A, B and C coincide by construction.
