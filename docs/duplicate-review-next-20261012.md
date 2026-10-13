# Next data task — duplicate-story review, October 12

The first review of the currently included 18 versions is closed: 14 three-class
labels (7 negative, 5 positive, 2 neutral), 4 unresolved, 0 pending first review.
This is assisted development only. The original positive rebate answer remains
preserved; the human agreed to unresolved after discussion of promised payment/
hidden cost. Final decision 6ff51f0e-c79e-48c3-bbb4-7aee14382698 became available at
2026-10-12T10:08:22.193069+00:00. Earlier/exact-release cutoffs still retain the prior
14 labeled / 3 unresolved / 1 pending state. Prior 17 decisions remain unchanged.

Closure evidence: annotation-first-review-closure-20261012.json.
Full inventory with exact versions/subjects/reasons is archived at Docker
/app/data/research/sentiment-teaching-batch06-final-recorded-summary-20261012.json.
The current class-label export still contains only 14 labels, not 18.

## Read-only duplicate audit findings

The frozen 41-version snapshot contains 38 article IDs and 38 exact title strings.
Two articles have multiple versions (3 and 2 respectively). Their three successive
comparisons show unchanged headline text with changed publisher-reported timestamp
fields. These are source-item versions, not demonstrated rewritten headlines.
Both article families are in the not-yet-eligibility-reviewed part of the snapshot.
Do not count them as validated financial syndication.

Reference grouping on all41 yields 38 article/exact/lexical groups. On the currently
included18, all three reference methods yield 18 groups. No distinct-article pair
reaches lexical Jaccard 0.75 in the 24-hour window. This does not prove absence of
real duplicates: lexical matching can miss semantic rewrites. It shows the current
rules/sample have not established a duplicate-control result for the included cohort.

Appending later rows left the first up-to-20 reference assignments unchanged in
all six scope/method checks. This is a check of deterministic forward replay, not
human ground truth. The audit ran on October12; old data availability times are
replayed arrival times, not backdated actual grouping judgments.

All41 inventory and included18 scope are kept separate. Current annotation history,
eligibility ledger, dataset and manifest hashes remained unchanged. No rows were
merged/deleted, labels transferred, predictions or model scores generated.

Local audit summary: duplicate-inventory-20261012.json.
Full reference memberships and eight screening pairs: Docker
/app/data/research/duplicate-inventory-20261012.json.
Its hash is recorded in the summary. This is not a benchmark of throughput or
accuracy, and the 38 reference groups are not an independently established event count.

## Today's next steps

1. Learn the difference between repeated delivery, the same article with a changed
   source-item version, and separate articles describing one underlying event.
2. Review real candidate pairs. Record same event / different event / uncertain,
   event identity, supporting text/source time, exact versions, reason and actual
   review availability. Shared wording or similar topic alone is insufficient.
   Preserve uncertain pairs and avoid forcing a group.
3. Build a development sample with verified same-event rewrites and hard negatives.
   Independently search missed pairs; nearest lexical matches are a biased screening
   sample and cannot estimate recall. Review additional21 eligibility records before
   treating that part of the snapshot as the sentiment corpus.
4. Use the audit to choose a justified source panel and collection/review plan.
   GDELT failures and source gaps remain open. A larger representative pilot and
   independent review are required before final protocol/model/testing freeze.

First practice pair from stored source headlines (human judgment blank):
- The September jobs report will be released Friday. Here's what to expect
- The Fed's main inflation measure will be released Wednesday. Here's what to expect

These are different URLs. Their lexical score is about 0.395, below 0.75. Similar
announcement phrasing is a screening clue, not a shared-event label. Ask the human
to name each described event and decide same/different/uncertain. Do not claim that
review has happened until the answer is actually recorded.

Eligibility timestamp hardening, independent annotation/adjudication, checkpoint
contamination/provenance, immutable predictions, full scoring and statistical
sample-size planning still remain before final testing. The current exercise is
not the intended 50–100-example pilot, a paper-ready dataset or completed research.


## First duplicate-pair learning response — recorded at 17:27 Bangkok

The human judged the jobs-report announcement and inflation-report announcement
as different events. The original source/weekday rationale is preserved. Verified
source context identifies both article publishers as www.cnbc.com; "Fed" is the
organization discussed in the second title, not a different article publisher.
Assistant feedback identifies the clearer event-topic distinction: employment
versus inflation. That feedback is not falsely attributed as a new human statement.
Different publishers or posting weekdays alone do not establish different events.

Response: /app/data/research/duplicate-pair01-response01-20261012.json.
Post-content-persistence availability marker:
/app/data/research/duplicate-pair01-response01-availability-20261012.json.
Local evidence: docs/duplicate-pair01-review-20261012.json.
Decision 618a7f85-bc07-4800-8952-54ef14ac5022 became available at
2026-10-12T10:27:39.062761+00:00 after response contents were flushed/fsynced.
The marker commits separately. Dataset, manifest, eligibility, annotation ledger
and original audit hashes remain unchanged; no cluster or classifier label changed.
This is an assisted development response, not independent adjudicated ground truth
or an estimate of grouping precision/recall.
Next learning check: two publishers report the same specific employment release
on different days; decide whether that alone creates a different underlying event.


## Second candidate-pair response — recorded at 17:50 Bangkok

The human judged the two September-employment articles different and noted their
different outcomes/tones. Original reasoning is preserved, not replaced. Captured
feed context identifies the first as a BLS nonfarm-payrolls/unemployment release;
the second headline identifies an ADP private-sector jobs release. Both article
publishers are CNBC. The stronger difference is underlying report identity.

Assistant feedback: shared month/topic and different sentiment alone do not prove
same or different event. Different financial subjects/angles can produce different
sentiments about one event. Keep same-event relation separate from same-information
relation; new facts or another angle must remain as their own input version, label,
provenance and availability even if assigned to one event family.

Response: /app/data/research/duplicate-pair02-response01-20261012.json.
Availability marker: /app/data/research/duplicate-pair02-response01-availability-20261012.json.
Local evidence: docs/duplicate-pair02-review-20261012.json.
Decision 594d9970-87d1-487d-b3ed-c4eb820a7418 became available at
2026-10-12T10:50:39.355732+00:00 after content flush/fsync. The marker is separate.
The captured summary was used only for event identity, not to change the headline-
only sentiment labels. Dataset, manifest, annotation/eligibility ledgers and audit
hashes stayed unchanged. No live dedup, cluster or label changed.

This is an assisted development response with source-context feedback, not independent
adjudicated ground truth or measured duplicate recall. We still need genuine same-event
rewrites, updates and independently searched missed pairs, not only negative examples.
Teaching examples of profits rising while shares fall are hypothetical and cannot be
recorded as real-feed validation or added to experiment samples.
