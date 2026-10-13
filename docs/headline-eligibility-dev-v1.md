# Financial-headline eligibility — development policy v1

This policy is for today's development pilot, not a frozen final-test protocol.
Review the exact saved headline version without model sentiment predictions or
subsequent market returns. Use the source item only to establish headline provenance.
Record a human reviewer identifier and a short reason for every judgment.

Assess these three questions independently, using `yes`, `no`, or `uncertain`:

1. **English:** Is the headline understandable English? Proper names, ticker
   symbols and quoted foreign words do not by themselves disqualify it. For
   mixed languages, garbled text or insufficient context, choose uncertain.
2. **Financially relevant:** Does the headline concern markets, businesses,
   corporate financial activity, economic indicators, public finances, or
   economic policy? General entertainment/sports/lifestyle alone is no. A clear
   financial/economic consequence qualifies; mere presence of a company name
   does not guarantee relevance. Borderline cases are uncertain.
3. **Genuine headline:** Is it a meaningful news headline supplied by the feed
   or headline API? URL slugs, navigation text, empty placeholders and synthetic
   titles constructed from filing identities are no. An authentic question,
   opinion headline or older headline can qualify; age alone is not exclusion.

All three yes means included. Any no means excluded. Otherwise the version
remains uncertain and needs adjudication. Missing reviews remain pending.
These judgments do not assign positive/neutral/negative sentiment. Mixed sentiment
and entity-dependent sentiment will be handled in the separate annotation guide.

Do not exclude a headline just because it is repeated or appears difficult for
FinBERT. Preserve duplicates and revisions for the experiment's grouping controls.
Keep original source/observation/storage times; eligibility does not change them.

Corrections append a new decision referring to the old decision ID. The tool
records actual decision availability at save time, conservatively later than the
human reading time. It accepts no backdated availability. A policy edit changes
its hash and requires new reviews under the new policy. Historical selections use
only data and decisions strictly before their cutoff. Today's reviews therefore
cannot be asserted as information available at an older historical cutoff.

Single-annotator judgments remain a limitation. Document uncertain cases and seek
independent review of a subset before freezing the final protocol.
