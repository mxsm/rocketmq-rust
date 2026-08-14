# OPT-02B: request ID relaxed-ordering experiment

## Hypothesis and audit

Request IDs are unique correlation values. Producers copy the fetched integer into `RemotingCommand::opaque`; consumers serialize it, expose it through getters, or use it to match requests and responses. No code performs an acquire load from the request-ID atomic or relies on the ID allocation to publish other memory. `Ordering::Relaxed` is therefore sufficient for uniqueness and signed wrap semantics.

The experiment changed only `next_request_id_from` from `Ordering::AcqRel` to `Ordering::Relaxed`. Static ownership, callers, construction, wire decode, tests, and benchmark code were unchanged from OPT-02A.

## Short A/B result

Both runs used the same machine and the same diagnostic profile: 1 second warmup, 1 second measurement, and 10 samples. They ran consecutively, but were not a repeated interleaved formal profile.

| Threads | AcqRel M commands/s | Relaxed M commands/s | Delta |
|---:|---:|---:|---:|
| 1 | 38.16 | 38.06 | -0.3% |
| 2 | 56.87 | 54.39 | -4.4% |
| 4 | 57.85 | 55.79 | -3.6% |
| 8 | 42.97 | 40.70 | -5.3% |
| 16 | 38.29 | 37.70 | -1.5% |
| 32 | 37.40 | 36.44 | -2.6% |

No concurrency point improved. The differences may include normal session drift, but they provide no evidence for the expected gain.

## Decision

Stop and roll back the candidate. The production helper remains `Ordering::AcqRel`, so this PR changes no runtime behavior. Although the audit supports Relaxed correctness, weakening a compatibility-sensitive atomic contract without a measured benefit is not justified. A future retry requires a new interleaved formal A/B with a positive high-concurrency result.

## Validation and rollback

The existing 8,192-ID concurrency test and signed-wrap test passed for the candidate and again after rollback. Since candidate code is absent from the final tree, rollback consists only of reverting this report.
