# Topic Design QA

## Evidence

- Source visual truth: `%TEMP%\rocketmq-topic-task9-qa\java-topic-list-1280x720.png` and `%TEMP%\rocketmq-topic-task9-qa\java-topic-create-1280x720.png`, captured from the local Java Topic page. These temporary screenshots are QA evidence and are not committed.
- Browser-rendered implementation: `%TEMP%\rocketmq-topic-task9-qa\rust-topic-list-1280x720.png`, `%TEMP%\rocketmq-topic-task9-qa\rust-topic-create-1280x720.png`, `%TEMP%\rocketmq-topic-task9-qa\rust-topic-list-768x1024.png`, and `%TEMP%\rocketmq-topic-task9-qa\rust-topic-create-768x1024.png`, captured from the local Rust Topic page in the selected in-app browser. These screenshots are not committed.
- Single full-view comparison input: `%TEMP%\rocketmq-topic-task9-qa\java-rust-topic-comparison-1280x720.png` (Java left, Rust right; list above, create form below; not committed).
- Viewport: both desktop pages reported the same `1280 x 720` CSS viewport with `devicePixelRatio = 1.5`. The browser capture surface produced `1253 x 705` source pixels for both Java states, `1265 x 712` implementation pixels for the Rust list, and `1280 x 720` implementation pixels for the Rust create dialog. The single comparison downsampled each capture to `620 x 349` solely to place the four states in one equal-size grid. Responsive implementation evidence used a controlled `768 x 1024` frame inside the same in-app browser.
- State: Java Topic list and Add Topic form; Rust Topic inventory and Create topic form, plus the Rust list and form at the narrow breakpoint.
- Focused region comparison: no extra crop was needed because form labels, inputs, focus rings, borders, and the complete visible inventory rows remained legible in the native full-view captures and in the combined input.

## Findings

- No actionable P0, P1, or P2 mismatch remains. The Rust page intentionally applies the approved dark monochrome operations-dashboard direction rather than copying the Java page's legacy light styling.
- Typography and hierarchy are coherent: compact system text, monospaced operational identifiers, clear page and dialog headings, and readable secondary metadata.
- Spacing and density are appropriate for an operations surface. The Rust inventory exposes filters, target metadata, pagination, and actions without the sparse dead space present in the Java reference.
- Borders, dark tokens, field focus, disabled actions, and destructive affordances remain distinguishable. Source scans found no gradient, hard-coded white, migration/parity copy, or raw Topic `<button>` markup.
- The narrow view has no page-level horizontal overflow (`clientWidth = scrollWidth = 753`); the wide operational table owns its horizontal overflow (`clientWidth = 703`, `scrollWidth = 1120`), and the create form resolves to one column.
- The screen uses coherent Lucide interface icons and no product imagery. There was no source imagery to reproduce.
- Rust page console error/warning capture returned an empty list. The Java reference emitted only existing Ant Design deprecation/form warnings and was not modified.

## Primary interactions tested

- All eight Topic type/category filters and Reset, including truthful empty results.
- Create and Edit against one disposable Topic, including cluster target discovery, FIFO/ordered configuration, and four read/write queues.
- Send test message with structured result (`SEND_OK`, broker, queue, offset, identifiers, region, and transaction state).
- Details: Overview, Routes/status, Consumers, and Configuration.
- Consumer reset and skip safety with no discovered consumer group: no arbitrary default and Continue disabled.
- Broker delete safety with exactly one authoritative broker and exact-name confirmation.
- Whole-topic delete exact-name confirmation boundary. The operator explicitly chose to retain the disposable QA Topic, so the final destructive click and cleanup assertion were skipped by user choice rather than by a product failure.
- System Topic action boundary: view-only.

## Comparison history

- Initial deterministic RED evidence: the Topic CSS had no scoped `min-width: 0`, local table overflow, dense result grids, or `<= 900px` one-column behavior required by the design brief.
- Fix: added scoped dense dark workspace rules in `src/styles/globals.css` for responsive toolbar wrapping, form/result grids, operation target rows, configuration alerts, and table-local overflow.
- Post-fix evidence: the desktop and narrow screenshots listed above. No P0/P1/P2 issue was found after the implementation pass, so no component behavior changed during this QA task.

final result: passed
