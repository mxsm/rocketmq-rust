# Mapped-file lifecycle sidecar format v1

Status: normative M3 format specification

On-disk version: `1.0`

Applies to: Issue #9143 managed mapped-file creation and retirement

This document is the sole byte-level source of truth for the v1 mapped-file lifecycle sidecar. The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. An implementation must not issue a durable retirement capability until its encoder, decoder, replay, and platform policy conform to this document and the v1 golden fixtures.

The sidecar does not change CommitLog, ConsumeQueue, Index, HA, or message bytes. It records only Store-local file incarnation, namespace, and retirement state. It never records mmap or `FileOwner` destruction as a durable deletion stage.

## 1. Safety contract

The format protects these invariants:

1. A queue identity is not forgotten before an acknowledged `RetirementIntent` frame exists, or exact-incarnation absence has been reconciled into an equivalent private proof.
2. A persisted path is never sufficient deletion authority. Every namespace operation also verifies `StoreUuid`, `FileIncarnationId`, and the currently opened handle's `PhysicalFileKey`.
3. A stale ticket cannot rename, delete, or suppress a replacement file at the same path.
4. Sequence ambiguity, identity-changing duplicate records, unsupported critical data, and non-tail corruption fail closed before numeric segments are published.
5. A trailing byte suffix can be repaired only when the acknowledgement slots and seals prove every byte in it was never acknowledged. The predecessor is never truncated in place.
6. `Completed` proves namespace/index convergence for one incarnation. It does not prove that mappings, handles, or leases have been dropped.

CRC-32 detects accidental corruption; it is not an authenticity mechanism. The threat model does not include an attacker able to rewrite sidecar bytes and recompute checksums. Filesystem permissions and the Store exclusive lock remain mandatory.

## 2. Directory layout and names

The lifecycle directory is exactly `<store-root>/.rocketmq-lifecycle`. All sidecar names are ASCII and case-sensitive at the codec layer. A case-fold collision on a case-insensitive filesystem is corruption.

The lifecycle directory and every ancestor beneath the already-opened Store root MUST be opened without following links and MUST be a real directory, not a symlink, mount escape, junction, or reparse point. All sidecar operations are relative to that verified directory handle. A path-based current-working-directory lookup is not permitted.

| Path or pattern | Meaning |
|---|---|
| `.rocketmq-lifecycle/store.meta` | Immutable Store identity, exactly 64 bytes. |
| `.rocketmq-lifecycle/ENABLED.v1` | Activation and generation selector, exactly 208 bytes (two 104-byte slots). It is created last. |
| `.rocketmq-lifecycle/ACKNOWLEDGED.v1` | Exact durable log-tail watermark, exactly 208 bytes (two 104-byte slots). |
| `.rocketmq-lifecycle/manifest.snapshot.g{G:020}` | Immutable snapshot generation `G`, where `G` is a zero-padded decimal `u64`. |
| `.rocketmq-lifecycle/retirement.log.g{G:020}` | Append-only log generation `G`. Snapshot and log generations are paired one-to-one in v1. |
| `.rocketmq-lifecycle/quarantine/` | Evidence retained after explicit ownership review. It is never recursively deleted. |
| `.rocketmq-lifecycle/quarantine/retirement.log.g{G:020}.tail.o{O:020}.l{L:020}.c{C:08x}.bin` | Exact unacknowledged suffix copied during tail repair; `O` is its start, `L` its byte length, and `C` its CRC-32. |
| `*.tmp.{N}` in the lifecycle directory | In-progress sidecar write; `N` is a 32-character lowercase hexadecimal nonce. It is never selected as durable state. |

There are no unversioned `manifest.snapshot` or `retirement.log` files in v1. Those names in earlier planning documents were placeholders. Generation 0 is named `manifest.snapshot.g00000000000000000000` and `retirement.log.g00000000000000000000`.

Lifecycle-created segment names are:

```text
canonical:  {segment_offset:020 decimal}
create:     .create.i{create_seq:016 lowercase hex}.s{segment_offset:020 decimal}.n{nonce:032 lowercase hex}
tombstone:  .delete.t{ticket_id:016 lowercase hex}.i{create_seq:016 lowercase hex}.s{segment_offset:020 decimal}.m{mapping_generation:016 lowercase hex}.n{nonce:032 lowercase hex}
```

`create_seq`, `ticket_id`, and `mapping_generation` are nonzero. A create file or tombstone stays in the same directory as its canonical segment. Cross-directory rename is forbidden. A syntactically similar name that does not decode exactly is unknown namespace data: retain it and block writable publication for the affected directory until it is audited.

Final generation files are created with exclusive create. A final name that already exists must decode to the exact expected generation, length, and CRC; otherwise activation fails closed. Temporary files are never treated as current merely because their generation is high.

## 3. Common encoding and limits

All multibyte integers are unsigned and little-endian. There is no implicit padding. Byte offsets in this document start at zero. Reserved bytes and unknown flag bits MUST be zero on encode and MUST cause v1 decoding to fail.

| Item | v1 encoding and limit |
|---|---|
| `StoreUuid` | 16 opaque bytes; all-zero is invalid. |
| `FileIncarnationId` | `StoreUuid[16]` followed by nonzero `create_seq: u64`; 24 bytes. |
| `TicketId` | Nonzero `u64`, monotonically allocated per Store and never reused. |
| nonce/bootstrap id | 16 opaque random bytes; all-zero is invalid. |
| `sequence` | Nonzero `u64`, globally monotonic across the selected generation chain; never reset by compaction. |
| generation | `u64`, starts at 0 and increases by exactly one. |
| timestamp | `u64` Unix nanoseconds; informational only and never used for authorization or replay ordering. |
| path | `length: u16` followed by exactly `length` UTF-8 bytes. A required path has length 1..4096; an explicitly optional path uses length 0. |
| log header | 40..256 bytes; v1 emits 40. |
| log payload | At most 16,384 bytes. |
| log frame | At most 16,644 bytes (`256 + 16,384 + 4`). |
| commit seal | Exactly 72 bytes after every acknowledged frame. |
| sealed record unit | At most 16,716 bytes (frame plus seal). |
| snapshot body | At most 268,435,456 bytes (256 MiB). |
| snapshot entries | At most 1,000,000; each payload at most 16,384 bytes. |

Identifier zero is invalid for store UUIDs, bootstrap/nonces, create sequences, ticket ids, mapping generations, record sequences, marker epochs, and acknowledgement epochs. A create/ticket high-water value of zero means that no such identifier has yet been allocated; generation zero and the explicitly documented sentinel/zero fields remain valid. Exhausting a monotonic `u64` domain stops lifecycle writes; wrapping or reuse is forbidden.

Version matching is exact in v1: every fixed sidecar and frame must carry major 1, minor 0. A v1.0 reader does not optimistically accept a later minor version. A future release may document a compatible minor and feature-bit rule, but that is a new reviewed reader contract. Unknown marker features, major/minor versions, or known-record versions fail closed. The only v1 forward-skipping rule is the explicitly noncritical unknown-record rule in section 6.

### 3.1 Store-relative path

A `StoreRelativePath` is byte-exact, well-formed UTF-8 using `/` separators. The codec does not perform Unicode normalization or case folding. Every encoder and decoder MUST enforce:

- not empty, not absolute, and no leading or trailing `/`;
- no NUL, backslash, colon, ASCII control character, empty component, `.` component, or `..` component;
- no Windows drive, UNC, verbatim/device prefix, alternate data stream, or component ending in a dot or space;
- no Windows reserved device component (`CON`, `PRN`, `AUX`, `NUL`, `COM1`..`COM9`, `LPT1`..`LPT9`) under ASCII case-insensitive comparison, including a suffix after `.`;
- encoded length at most 4096 bytes and each component at most 255 UTF-8 bytes;
- decoding and re-encoding produces the identical byte sequence.

String equality is not a namespace authorization check. Operation code resolves the path beneath a canonical, exclusively reserved Store root, refuses symlink/reparse-point traversal, opens the target without following links, and compares its physical key. Windows case aliases therefore cannot be accepted or rejected only by string comparison.

### 3.2 Physical file key

`PhysicalFileKey` is a fixed 32-byte value:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 1 | kind: `1 = Unix`, `2 = Windows`. |
| 1 | 7 | zero. |
| 8 | 8 | Unix `device`, or Windows `volume_serial`. |
| 16 | 8 | Unix `inode`, or the first 8 bytes of Windows `FILE_ID_128.Identifier`. |
| 24 | 8 | zero for Unix, or the final 8 bytes of Windows `FILE_ID_128.Identifier`. |

The 16 Windows identifier bytes are stored in API byte order; they are not interpreted as an integer. A Unix key is obtained from the already-opened no-follow handle (`fstat`), not from a prior path lookup. A Windows key is obtained with `GetFileInformationByHandleEx(FileIdInfo)`.

No physical-key component has a nonzero invariant: zero is a valid OS-returned `device`, `inode`, `volume_serial`, or file-id byte pattern. Where a containing record permits an absent key, its explicit presence flag distinguishes absence from an all-zero but present key.

Physical keys are point-in-time binding evidence, not permanent global identifiers. Unix inode/device pairs can be reused. Microsoft documents that `VolumeSerialNumber + FILE_ID_128` identifies and compares files on one computer; it is not a cross-machine identity. Backup restore, filesystem migration, cloning, and filesystem behavior can change or reuse identifiers. Such a mismatch requires explicit offline rebind/restore handling and is never silently written into the ledger.

## 4. Checksums and hashes

Every `crc32` in v1 is CRC-32/ISO-HDLC (commonly CRC-32 IEEE):

| Parameter | Value |
|---|---|
| width | 32 |
| normal polynomial | `0x04C11DB7` |
| reflected polynomial | `0xEDB88320` |
| init | `0xFFFFFFFF` |
| refin / refout | `true / true` |
| xorout | `0xFFFFFFFF` |
| check value for ASCII `123456789` | `0xCBF43926` |
| serialized integer | little-endian `u32` |

CRC coverage is stated separately for each structure. A CRC field never covers itself. CRC-32 is the only checksum implemented by the on-disk codec. SHA-256 is used only by fixture/evidence tooling and is never encoded in v1. Golden `.sha256` files contain the lowercase FIPS 180-4 digest of the complete fixture, two spaces, the fixture basename, and LF.

## 5. Fixed sidecar structures

### 5.1 `store.meta`

`store.meta` is immutable after bootstrap and exactly 64 bytes.

| Offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMSM` (`52 4d 53 4d`). |
| 4 | `u16` | major `1`. |
| 6 | `u16` | minor `0`. |
| 8 | `u32` | total length `64`. |
| 12 | `u32` | flags `0`. |
| 16 | `[u8; 16]` | nonzero `store_uuid`. |
| 32 | `u64` | creation time, Unix nanoseconds. |
| 40 | `[u8; 16]` | nonzero bootstrap id. |
| 56 | `u32` | zero. |
| 60 | `u32` | CRC-32 over bytes `[0, 60)`. |

A different valid `store.meta` must never be adopted for an existing marker, snapshot, or log. UUID or bootstrap-id mismatch is fatal.

### 5.2 `ENABLED.v1`

The activation marker is a 208-byte file containing slot 0 at `[0,104)` and slot 1 at `[104,208)`. An unused slot is all zero and invalid. Each populated slot is:

| Relative offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMEN` (`52 4d 45 4e`). |
| 4 | `u16` | major `1`. |
| 6 | `u16` | minor `0`. |
| 8 | `u16` | slot length `104`. |
| 10 | `u8` | physical slot index, 0 or 1. |
| 11 | `u8` | flags; exactly bit 0 (`ENABLED`) set. |
| 12 | `u32` | required feature bitmap; v1 is exactly `0x00000001` (`MANAGED_RETIREMENT`). |
| 16 | `[u8; 16]` | `store_uuid`. |
| 32 | `[u8; 16]` | bootstrap id from `store.meta`. |
| 48 | `u64` | nonzero marker epoch. |
| 56 | `u64` | selected snapshot generation. |
| 64 | `u64` | selected log generation; equal to snapshot generation in v1. |
| 72 | `u64` | anchor frame sequence. |
| 80 | `u64` | exact selected snapshot file length. |
| 88 | `u32` | CRC-32 over the complete selected snapshot file, including its stored CRC fields. |
| 92 | `u32` | CRC-32 over the complete anchor frame, including its stored CRC fields. |
| 96 | `u32` | zero. |
| 100 | `u32` | CRC-32 over the first 100 bytes of this slot. |

Selection validates both slots independently. The valid slot with the greater epoch is authoritative. When both slots are valid their epochs must differ by exactly one; equal or nonconsecutive epochs are fatal. The all-zero bootstrap slot is the only invalid slot that is known unused from its bytes alone. Any other invalid/torn slot requires the higher-generation scan in section 10.1 before an older valid slot can be selected. Once the highest valid slot passes its own CRC, a missing or invalid referenced pair is fatal; the implementation MUST NOT fall back, because acknowledged frames may exist only in that pair.

At bootstrap, slot 0 has epoch 1 and slot 1 is zero. A generation switch writes the inactive slot with `epoch + 1` using positional `write_all`, calls `sync_all`/`FlushFileBuffers` on the existing marker file, and re-reads that slot. It then acknowledges and seals the already-synced `LogOpened` anchor, appends/acknowledges/seals `MarkerCommitted`, and only then allows ordinary frames in the new log. The directory entry is not replaced during ordinary generation changes. This two-slot rule is part of the crash protocol, not an optimization.

### 5.3 `ACKNOWLEDGED.v1`

The acknowledgement file is a 208-byte file with slot 0 at `[0,104)` and slot 1 at `[104,208)`. It is created at its final name before any log frame is acknowledged and is never renamed during normal operation. An unused slot is all zero. A populated slot is:

| Relative offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMAC` (`52 4d 41 43`). |
| 4 | `u16` | major `1`. |
| 6 | `u16` | minor `0`. |
| 8 | `u16` | slot length `104`. |
| 10 | `u8` | physical slot index, 0 or 1. |
| 11 | `u8` | flags: bit 0 `ACTIVATED`; all other bits zero. |
| 12 | `u32` | zero. |
| 16 | `[u8; 16]` | store UUID. |
| 32 | `[u8; 16]` | bootstrap id. |
| 48 | `u64` | nonzero acknowledgement epoch. |
| 56 | `u64` | marker epoch; 0 before activation, otherwise the selected marker epoch. |
| 64 | `u64` | acknowledged log generation. |
| 72 | `u64` | acknowledged frame sequence. |
| 80 | `u64` | end offset of the acknowledged frame, which is also the start offset of its seal. |
| 88 | `u32` | CRC-32 over the complete frame, including its stored header/payload CRC fields. |
| 92 | `u64` | expected sealed log length, exactly frame end + 72. |
| 100 | `u32` | CRC-32 over the first 100 bytes of this slot. |

Slot epochs increase by exactly one and alternate physical slots. When both are valid, their epochs must be consecutive; the higher epoch is authoritative. The slot's generation/sequence/offset/frame CRC and the corresponding commit seal must agree exactly. Before activation, flags and marker epoch are zero. Once any valid slot has `ACTIVATED`, all future slots must retain it and carry the selected marker epoch; clearing it is corruption.

An invalid nonzero acknowledgement slot is never silently ignored. Recovery enumerates commit seals reachable through the validated selected/predecessor generation chain. A reconstruction candidate must name the invalid slot's physical index, have the parity required by epoch 1 in slot 0 and strict alternation, be exactly one epoch before or after the other valid slot, and form the highest complete consecutive epoch/sequence chain with that slot. Its preceding frame must supply matching generation/sequence/end/CRC, and the recomputed 104-byte slot CRC must equal the value stored in the seal. Exactly one candidate is required. Zero candidates, multiple candidates, a candidate outside the highest continuous chain, or any missing/intermediate chain unit fails closed. This rule prevents an older historical seal or watermark from hiding an acknowledged suffix.

### 5.4 Snapshot header

A snapshot is immutable after final-name publication. It consists of a 104-byte header, `body_len` bytes of entries, then a 4-byte body CRC. Its exact total is `108 + body_len`.

| Offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMSN` (`52 4d 53 4e`). |
| 4 | `u16` | major `1`. |
| 6 | `u16` | minor `0`. |
| 8 | `u16` | header length `104`. |
| 10 | `u16` | flags: bit 0 bootstrap inventory, bit 1 tail repair; not both. Zero means ordinary compaction. |
| 12 | `u64` | total file length, exactly `108 + body_len`. |
| 20 | `[u8; 16]` | `store_uuid`. |
| 36 | `u64` | snapshot generation. |
| 44 | `u64` | paired log generation, equal to snapshot generation. |
| 52 | `u64` | predecessor log generation; `u64::MAX` for generation 0. |
| 60 | `u64` | base sequence represented by this snapshot. |
| 68 | `u64` | allocated create-sequence high-water mark. |
| 76 | `u64` | allocated ticket high-water mark. |
| 84 | `u32` | entry count. |
| 88 | `u64` | body length. |
| 96 | `u32` | zero. |
| 100 | `u32` | header CRC over bytes `[0, 100)`. |
| 104 | bytes | body. |
| `104 + body_len` | `u32` | CRC-32 over the body only. |

Snapshot entries are ordered canonically: incarnation entries by `(store_uuid, create_seq)`, retirement-ticket entries by `ticket_id`, then quarantine entries by source-path bytes. Duplicate keys, a high-water below a represented ID, or a noncanonical order are corruption.

Each snapshot entry is `kind: u16`, `version: u16` (1), `payload_len: u32`, payload, and `entry_crc32: u32`. The entry CRC covers its 8-byte header and payload. Kinds are `1 = Incarnation`, `2 = RetirementTicket`, `3 = Quarantine`. Unknown kinds fail closed in v1.

#### Incarnation snapshot payload (kind 1)

| Offset | Type | Meaning |
|---:|---|---|
| 0 | `[u8; 16]` | store UUID. |
| 16 | `u64` | create sequence. |
| 24 | `u8` | phase: 1 Allocated, 2 Bound, 3 Published. |
| 25 | `u8` | bit 0 means the physical key is present. |
| 26 | `u16` | zero. |
| 28 | `u64` | segment offset. |
| 36 | `u64` | expected file length. |
| 44 | `[u8; 16]` | create nonce. |
| 60 | `PhysicalFileKey[32]` | zero bytes only in Allocated phase; otherwise present and bit 0 set. |
| 92 | `path` | canonical path. |
| next | `path` | create-file path. |

Allocated phase requires flag bit 0 clear and all 32 key bytes zero; the decoder does not parse a key tag in that absent encoding. Bound and Published require bit 0 set and a valid kind-1 or kind-2 key. Every other phase/flag/key combination is corruption.

#### Retirement-ticket snapshot payload (kind 2)

| Offset | Type | Meaning |
|---:|---|---|
| 0 | `u64` | ticket id. |
| 8 | `[u8; 16]` | store UUID. |
| 24 | `u64` | create sequence. |
| 32 | `u8` | durable stage: 1 IntentDurable, 2 LogicalRemoved, 3 Tombstoned, 4 NamespaceAbsent, 5 Completed-retained. |
| 33 | `u8` | bit 0 tombstone path present, bit 1 SupersededPath observed, bit 2 quarantined. |
| 34 | `u16` | retirement reason. |
| 36 | `u64` | sequence of the frame that established the current durable stage. |
| 44 | `u64` | mapping generation. |
| 52 | `u64` | segment offset. |
| 60 | `u64` | expected file length. |
| 68 | `[u8; 16]` | retirement nonce. |
| 84 | `PhysicalFileKey[32]` | target key. |
| 116 | `path` | canonical path. |
| next | optional `path` | tombstone path; zero length iff flag bit 0 is clear. |

The stage sequence is nonzero, no greater than the snapshot base sequence, and must identify the exact frame that established the stored stage; for stage 5 it is the `Completed` frame sequence. If a tombstone path was ever authorized, flag bit 0 and that exact path remain present through stages 4 and 5 even after absence was observed. A Completed-retained entry therefore preserves the exact canonical path, physical key, and every authorized tombstone path needed for post-crash namespace revalidation. It remains required until a later clean startup has revalidated that exact incarnation and every authorized tombstone; only a subsequent compaction may omit it.

#### Quarantine snapshot payload (kind 3)

| Offset | Type | Meaning |
|---:|---|---|
| 0 | `u8` | entity kind: 1 create, 2 tombstone, 3 sidecar, 4 canonical. |
| 1 | `u8` | reason: 1 unknown owner, 2 key mismatch, 3 malformed name, 4 restore/rebind required. |
| 2 | `u16` | bit 0 key present, bit 1 content fingerprint present, bit 2 destination present. |
| 4 | `u64` | sequence at observation. |
| 12 | `PhysicalFileKey[32]` | zero when absent. |
| 44 | `u64` | content length, zero when absent. |
| 52 | `u32` | CRC-32 of the exact content, zero when absent. |
| 56 | `u32` | zero. |
| 60 | `path` | source path. |
| next | optional `path` | quarantine destination. |

When the quarantine key-present flag is clear, all 32 key bytes must be zero and their zero kind byte is not decoded as a `PhysicalFileKey`. When it is set, the key tag must be 1 or 2. Content and destination fields follow the same explicit-present/all-zero-or-empty rule.

## 6. Ledger frame

Every known v1 log frame has a 40-byte header, payload, and 4-byte payload CRC. The generic layout uses the encoded `header_len` so a supported reader can bound and skip an unknown noncritical record.

| Offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMLC` (`52 4d 4c 43`). |
| 4 | `u16` | format major `1`. |
| 6 | `u16` | format minor `0`. |
| 8 | `u16` | record type. |
| 10 | `u16` | record version `1`. |
| 12 | `u16` | flags; bit 0 `CRITICAL`. Every record defined here emits exactly `1`. |
| 14 | `u16` | header length; v1 emits `40`. |
| 16 | `u32` | payload length. |
| 20 | `u64` | global sequence. |
| 28 | `u64` | containing log generation. |
| 36 | `u32` | header CRC. |
| `header_len` | bytes | payload; offset 40 for a known v1 record. |
| `header_len + payload_len` | `u32` | payload CRC. |

For a 40-byte header, header CRC covers `[0,36)`. If a later compatible reader accepts `header_len > 40`, coverage is `[0,36)` concatenated with `[40,header_len)`; the CRC field itself is excluded. Payload CRC covers exactly the payload. Total frame length is `header_len + payload_len + 4`.

A known v1 record requires `header_len = 40`. An unknown record may be skipped only when the sidecar format version is supported, `header_len` is 40..256, its full header/length/CRCs/sequence are valid, and `CRITICAL` is clear. Unknown critical records and unknown versions of known records fail closed. v1 writers never emit a noncritical record.

### 6.1 Commit seal

Every acknowledged frame is immediately followed by a 72-byte seal. The seal is not a frame, has no record type, and does not consume a sequence. Its layout is:

| Offset | Type | Required value / meaning |
|---:|---|---|
| 0 | `[u8; 4]` | magic `RMCS` (`52 4d 43 53`). |
| 4 | `u16` | major `1`. |
| 6 | `u16` | minor `0`. |
| 8 | `u16` | seal length `72`. |
| 10 | `u8` | acknowledgement slot index. |
| 11 | `u8` | acknowledgement flags copied exactly. |
| 12 | `u32` | zero. |
| 16 | `u64` | acknowledgement epoch. |
| 24 | `u64` | marker epoch. |
| 32 | `u64` | log generation. |
| 40 | `u64` | frame sequence. |
| 48 | `u64` | frame end offset / seal start offset. |
| 56 | `u32` | CRC-32 over the complete preceding frame. |
| 60 | `u32` | stored CRC-32 value of the exact acknowledgement slot. |
| 64 | `u32` | zero. |
| 68 | `u32` | seal CRC-32 over bytes `[0,68)`. |

For every normal committed unit, `seal_offset + 72` equals the authoritative acknowledgement slot's sealed-log length at the moment that unit was acknowledged. Older seals remain in the log and form a verifiable commit chain; the final seal must agree with the selected acknowledgement slot.

### 6.2 Record type values

| Value | Name | Payload |
|---:|---|---|
| `0x0001` | `StoreInitialized` | fixed 64 |
| `0x0002` | `BootstrapInstalled` | fixed 88 |
| `0x0003` | `LogOpened` | fixed 104 |
| `0x0004` | `GenerationPrepared` | fixed 56 |
| `0x0005` | `GenerationAborted` | fixed 48 |
| `0x0006` | `MarkerCommitted` | fixed 56 |
| `0x0010` | `AllocateIncarnation` | variable |
| `0x0011` | `BindIncarnation` | variable |
| `0x0012` | `PublishIncarnation` | variable |
| `0x0020` | `RetirementIntent` | variable |
| `0x0021` | `LogicalRemoved` | variable |
| `0x0022` | `Tombstoned` | variable |
| `0x0023` | `NamespaceAbsent` | variable |
| `0x0024` | `Completed` | fixed 56 |
| `0x0025` | `SupersededPath` | variable |
| `0x0030` | `Quarantined` | variable |

All other values are unassigned. `0x0000` is invalid.

Variable payloads contain exactly the stated fixed prefix and paths, with no alignment or trailing bytes. Their maximum encoded sizes are:

| Record | Fixed prefix | Paths | Maximum payload |
|---|---:|---:|---:|
| `AllocateIncarnation` | 56 | 2 required | 8,252 |
| `BindIncarnation` / `PublishIncarnation` | 64 | 2 required | 8,260 |
| `RetirementIntent` | 108 | 1 required | 4,206 |
| `LogicalRemoved` | 64 | 1 required | 4,162 |
| `Tombstoned` | 80 | 2 required | 8,276 |
| `NamespaceAbsent` | 76 | 1 required + 1 optional | 8,272 |
| `SupersededPath` | 96 | 1 required | 4,194 |
| `Quarantined` | 60 | 1 required + 1 optional | 8,256 |

The corresponding snapshot **payload** maxima are 8,288 bytes for Incarnation, 8,312 for RetirementTicket, and 8,256 for Quarantine. Including each entry's 8-byte header and 4-byte CRC, the complete entry maxima are respectively 8,300, 8,324, and 8,268 bytes. Every payload remains below the global 16,384-byte payload limit.

### 6.3 Administrative payloads

`StoreInitialized` (sequence 1 of generation 0): UUID `[0,16)`, bootstrap id `[16,32)`, creation time `u64` at 32, initial snapshot generation `u64 = 0` at 40, initial log generation `u64 = 0` at 48, and feature bitmap `u64 = 1` at 56.

`BootstrapInstalled`: UUID at 0, bootstrap id at 16, snapshot generation `u64` at 32, snapshot base sequence `u64` at 40, exact snapshot file length `u64` at 48, complete-file CRC-32 at 56, zero `u32` at 60, inventory count `u64` at 64, create high-water `u64` at 72, and ticket high-water `u64` at 80. Its count and high-water values must equal the referenced snapshot header/body. In generation 0 this is the marker's anchor frame.

`GenerationPrepared`: UUID at 0, source generation `u64` at 16, target generation `u64` at 24, target snapshot generation `u64` at 32, this frame's sequence repeated as `u64` at 40, open reason `u8` at 48 (`0 = compaction`), then seven zero bytes. Target generation is source + 1.

`GenerationAborted`: UUID at 0, source generation `u64` at 16, target generation `u64` at 24, matching `GenerationPrepared` sequence `u64` at 32, abort reason `u32` at 40 (`1 = I/O`, `2 = space`, `3 = operator cancellation`, `4 = validation`), then zero `u32` at 44. It is allowed only in the still-selected source log immediately after an unmatched `GenerationPrepared`; it releases the append barrier without selecting the target generation.

`MarkerCommitted`: UUID at 0, marker epoch `u64` at 16, selected snapshot generation `u64` at 24, selected log generation `u64` at 32, marker anchor sequence `u64` at 40, physical marker slot index `u8` at 48, three zero bytes at 49, and the selected slot's stored CRC-32 value `u32` at 52. This witness is written only after the marker slot was synced and re-read byte-for-byte. It is the first frame after the acknowledged/sealed `BootstrapInstalled` anchor in generation 0 or after the acknowledged/sealed `LogOpened` anchor in a later generation. Its acknowledgement slot has `ACTIVATED` set and names that marker epoch. No ordinary lifecycle frame may precede it.

`LogOpened`: UUID at 0; generation `u64` at 16; snapshot generation `u64` at 24; predecessor log generation `u64` at 32; predecessor terminal acknowledged sequence `u64` at 40; snapshot base sequence `u64` at 48; exact snapshot file length `u64` at 56; complete snapshot CRC-32 at 64; predecessor-prefix CRC-32 at 68; validated prefix length `u64` at 72; unacknowledged suffix length `u32` at 80; suffix CRC-32 at 84; open reason `u8` at 88 (`0 = compaction`, `1 = tail repair`); predecessor acknowledgement epoch `u64` at 89; and seven zero bytes at 97. Snapshot fields must match the snapshot header, complete-file CRC, and selected marker slot. The first seal in this generation has acknowledgement epoch `predecessor_ack_epoch + 1`. For compaction, the prefix ends after the acknowledged/sealed `GenerationPrepared` and the suffix length/CRC are zero. For tail repair, the predecessor is the exact acknowledged sealed prefix followed by exactly the recorded unacknowledged suffix. For either reason, this is the first frame in the new log and its sequence is `snapshot.base_sequence + 1`.

### 6.4 Incarnation payloads

`AllocateIncarnation` has UUID at 0, create sequence at 16, segment offset at 24, expected length at 32, create nonce `[40,56)`, canonical path at 56, then create-file path. Expected length is nonzero; the canonical basename and create-file basename must exactly encode the payload fields using section 2. It is valid only from no prior state for that reserved path, and its create sequence must equal the previous create high-water plus one. Applying it advances that high-water even if physical creation later fails.

`BindIncarnation` has UUID at 0, create sequence at 16, expected length at 24, physical key `[32,64)`, canonical path at 64, then create-file path. It requires an exact matching allocation.

`PublishIncarnation` uses the same payload layout as `BindIncarnation`. It requires Bound state, a same-directory rename, and post-rename no-follow handle verification of the same physical key and length. Only this durable frame permits active queue publication.

### 6.5 Retirement payloads

Retirement reason values are:

| Value | Meaning |
|---:|---|
| 1 | TTL expired |
| 2 | offset truncate |
| 3 | reset |
| 4 | delete last |
| 5 | explicit Store destroy |
| 6 | allocation/preallocation orphan |
| 7 | topic retirement |
| 8 | derived-file retirement |
| 9 | audited operator request |

Unknown reason values fail closed.

`RetirementIntent` has ticket id at 0, UUID at 8, create sequence at 24, reason `u16` at 32, flags `u16 = 0` at 34, mapping generation at 36, segment offset at 44, expected length at 52, retirement nonce `[60,76)`, target physical key `[76,108)`, and canonical path at 108. Mapping generation and expected length are nonzero, and the canonical basename must match the segment offset. The ticket id must equal the previous ticket high-water plus one and applying the frame advances that high-water. The create sequence must already exist and be no greater than the create high-water. Only successful completion of every acknowledgement step in section 8, including acknowledgement-slot sync/re-read and seal sync/re-read, may mint `DurableRetirementToken`.

`LogicalRemoved` has ticket id at 0, UUID at 8, create sequence at 24, target physical key `[32,64)`, and canonical path at 64. It requires `IntentDurable` and records consumption of the private handoff capability. Process-local ArcSwap/RCU pointer identity is intentionally not persisted.

`Tombstoned` has ticket id at 0, UUID at 8, create sequence at 24, target physical key `[32,64)`, retirement nonce `[64,80)`, canonical path at 80, then the required tombstone path. It requires `LogicalRemoved`. Repeating the physical rename after a crash is permitted only after reopening and matching the same target key.

`NamespaceAbsent` has ticket id at 0, UUID at 8, create sequence at 24, proof flags `u16` at 32, zero `u16` at 34, observation time at 36, target physical key `[44,76)`, canonical path at 76, then an optional tombstone path. Proof bit 0 means the targeted canonical incarnation is absent (the path is absent or is verified to contain a different incarnation); bit 1 means every authorized tombstone is absent; bit 2 means a replacement incarnation was observed. Bits 0 and 1 are required. A single unbound `NotFound` cannot produce this record.

`Completed` has ticket id at 0, UUID at 8, create sequence at 24, completion time at 32, the exact prerequisite `NamespaceAbsent` stage sequence at 40, proof flags `u32` at 48 (bits 0 and 1 required), and zero `u32` at 52. The prerequisite may be a live `NamespaceAbsent` frame or a kind-2 snapshot entry whose stage is NamespaceAbsent and whose stage-sequence field is identical. It also requires durable convergence of the replay-derived active index and retired filter. It authorizes runtime registry GC, not forced unmap, handle close, or immediate omission of its durable revalidation evidence.

`SupersededPath` has ticket id at 0, UUID at 8, create sequence at 24, expected target key `[32,64)`, observed replacement key `[64,96)`, and canonical path at 96. It is a sticky observation, not a retirement stage and not deletion authority.

`Quarantined` uses the kind-3 snapshot payload verbatim. It records only an explicit audited classification or move. Merely finding an unknown file does not authorize moving or deleting it.

## 7. Legal state machines

Incarnation transitions are strict and irreversible:

```text
none -> Allocated -> Bound -> Published
```

A crash can leave Allocated or Bound state. Recovery either completes publication after exact path/key/length checks or retains/quarantines it. It must not invent a `PublishIncarnation` from a filename alone.

Retirement transitions are:

```text
Published
  -> IntentDurable
  -> LogicalRemoved
  -> Tombstoned        (optional on Unix; normally required on Windows)
  -> NamespaceAbsent
  -> Completed
```

The Unix direct-unlink path is `LogicalRemoved -> NamespaceAbsent`. `SupersededPath` and `Quarantined` are sticky annotations and do not advance or regress the main stage. A later record may repeat an already reached stage only with identical identity and stage payload; it is idempotent. Stage regression, skipped required predecessors, two tickets claiming the same incarnation concurrently, a ticket changing path/key/nonce/reason, or an incarnation changing path/key/length is fatal corruption.

`Completed` is terminal in replay but initially remains a kind-2 snapshot candidate at stage 5. Completion in the current process is not sufficient to discard the ticket's path/key evidence. A later clean startup must replay it under the Store lock and positively revalidate the exact canonical incarnation, every authorized tombstone, and index/filter convergence. That startup marks the entry omission-eligible only in memory; a following compaction may then omit it. Until that sequence completes, compaction encodes the stage-5 entry unchanged.

Default path reuse is forbidden until the prior ticket is `Completed`. If an audited restore introduces a replacement despite that default, the stale ticket may record `SupersededPath`, may clean only its separately verified old tombstone, and must never touch or filter the replacement.

## 8. Append, acknowledgement, and namespace ordering

A durable frame acknowledgement uses this exact, non-batched protocol:

1. Encode one bounded frame, append it with `write_all` immediately after the previous seal, and `sync_all` the log.
2. Construct the next inactive acknowledgement slot with epoch + 1, the frame sequence/generation/end/CRC, and expected seal-end length. Write the complete 104-byte slot positionally, sync `ACKNOWLEDGED.v1`, and re-read it byte-for-byte.
3. Construct the deterministic 72-byte seal from that slot, append it at the recorded frame end, and `sync_all` the log again.
4. Re-read and validate the seal and exact EOF. Only then return durable acknowledgement or issue any capability derived from the frame.

Short write, interrupted retry exhaustion, either sync failure, invalid re-read, or ambiguous handle state does not acknowledge. A caller never infers success from an error. Replay uses the acknowledgement slot and seal rules below to finish a commit that crashed between steps, or fails closed. v1 does not batch multiple frames under one acknowledgement slot.

The mandatory retirement order is:

1. registry strongly owns the retiring entry;
2. append and acknowledge `RetirementIntent`;
3. construct the private non-Clone token;
4. atomically hand off/remove from the active queue and acknowledge `LogicalRemoved`;
5. detach mapping/file owner slots without forcing extant leases;
6. verify the path reservation and open-handle physical key;
7. rename/unlink/delete according to the platform policy;
8. perform the platform durability step and re-open/reverify namespace state;
9. append the corresponding monotonic stage;
10. append `Completed` only after canonical/tombstone absence and durable index convergence.

A namespace operation that succeeds while its following stage append fails is recovered from the prior durable intent plus actual namespace state. A stage must never be written in advance of its physical observation. Ledger or sync failure stops new capability issuance and reports degraded/backpressured storage; it does not remove active identity.

## 9. Replay and corruption

Replay occurs under the Store lifecycle/exclusive lock before any numeric segment scan or publication. A "frame end" excludes its seal; a "sealed length" includes the final 72-byte seal.

1. If any recognized lifecycle artifact or the external activation fence exists, legacy numeric scanning is disabled. Validate `store.meta` and `ACKNOWLEDGED.v1` whenever either exists. With no `ENABLED.v1`, a valid `ACTIVATED` acknowledgement slot or any valid sealed `MarkerCommitted` is fatal local activation-state loss; otherwise only the resumable bootstrap procedure in section 12 may open the artifacts.
2. Validate both acknowledgement slots. Starting with the highest valid epoch, inspect the exact frame/seal chain it names. Reconstruct an invalid nonzero slot only by the rule in section 5.3; if one valid slot has epoch greater than 1 while the other slot is all zero, fail closed because the zero slot can no longer be proved unused. Do not choose a lower watermark merely because its frame is easier to read.
3. If the marker exists, validate both marker slots and apply the higher-generation scan in section 10.1 before selecting a slot. Open only the snapshot/log pair it names and validate lengths before allocation, all CRCs, UUID/bootstrap/generation/base/high-water fields, snapshot ordering, anchor sequence, and marker-bound values.
4. Relate marker and acknowledgement state using the switch states below. Outside those explicitly recoverable states, the authoritative acknowledgement names the selected marker epoch and log generation; an acknowledgement ahead of the marker, behind by more than the one named predecessor, or bound to an unrelated epoch/generation fails closed.
5. Parse complete frame/seal units up to the authoritative `sealed_log_len`. Validate every frame and seal, consecutive record sequences, consecutive acknowledgement epochs, exact offsets, full-frame CRCs, stored acknowledgement-slot CRCs, and the final slot match. Generation 0 starts with `StoreInitialized` sequence 1 / acknowledgement epoch 1. A later generation starts with `LogOpened` at `snapshot.base_sequence + 1` and acknowledgement epoch `predecessor_ack_epoch + 1`.
6. Materialize snapshot state and apply frames whose sequence is greater than the snapshot base. Reconcile create files, canonical files, and tombstones using no-follow handles and physical keys.
7. Build the retiring registry, retired filter, and active incarnation index. Only then scan numeric segment names and publish entries having a durable Published incarnation and not filtered by a pending ticket.

The only recoverable marker/acknowledgement switch states are:

| Marker state | Acknowledgement/log state | Required recovery |
|---|---|---|
| bootstrap marker epoch 1 selects generation 0 | authoritative slot is pre-activation, generation 0, and the committed prefix ends at the acknowledged/sealed `BootstrapInstalled` anchor; any following bytes are an exact prefix of the deterministic `MarkerCommitted` frame | append only the missing witness-frame bytes, sync, then acknowledge/seal it with `ACTIVATED`; ordinary frames remain forbidden. |
| marker epoch `E` selects generation `G + 1` | authoritative slot still names predecessor `G`; new log is exactly one valid unsealed `LogOpened`, and its fields bind the selected snapshot, predecessor sequence, and predecessor acknowledgement epoch | acknowledge/seal that `LogOpened` with `ACTIVATED`, marker epoch `E`, and generation `G + 1`. |
| marker epoch `E` selects generation `G + 1` | authoritative slot names `E/G + 1` and the committed prefix ends at the sealed `LogOpened`; any following bytes are an exact prefix of the deterministic `MarkerCommitted` frame | append only the missing witness-frame bytes, sync, then acknowledge/seal the exact witness. |
| marker epoch `E` selects its generation | authoritative slot names `E` and ends at or after the sealed `MarkerCommitted` | normal replay, subject to the tail table. |

No application record is legal in a selected generation until its exact `MarkerCommitted` is acknowledged and sealed. The acknowledgement slot used for `LogOpened`, `MarkerCommitted`, and every later frame has `ACTIVATED` set and repeats the selected marker epoch.

| Byte/metadata condition | Required result |
|---|---|
| EOF exactly at the authoritative `sealed_log_len`, immediately after its matching seal | normal EOF. EOF immediately after a frame is never normal. |
| EOF before the authoritative final frame end | acknowledged frame loss; fail closed. |
| EOF equals the authoritative frame end or cuts its deterministic seal | require all available seal bytes to equal the deterministic prefix, append only the missing seal suffix, sync/re-read the log, and verify exact EOF; any mismatch fails closed. |
| a complete valid seal after the selected watermark has a higher consecutive acknowledgement epoch | reconstruct/select its exact acknowledgement slot, validate the intervening unit, and continue; never discard the sealed unit. |
| bytes follow the authoritative seal, both acknowledgement slots are valid and consecutive (or the other is the permitted initial all-zero slot), and no complete higher seal exists | the bytes are an unacknowledged suffix. It may contain at most one partial/complete frame and a partial seal, with total length less than 16,716 bytes; preserve it and use section 9.1. |
| bytes follow the authoritative seal and an acknowledgement slot is invalid nonzero, slot history is missing, a higher seal is ambiguous, or the suffix is too long | fail closed; acknowledgement cannot be disproved. |
| any frame/seal CRC, offset, length, generation, or slot-CRC mismatch inside acknowledged bytes | fail closed; the damaged unit may contain the only acknowledged intent. |
| invalid length, enum, flags, path, UTF-8, reserved byte, or present physical-key tag | fail closed before allocation/publication. |
| record-sequence or acknowledgement-epoch gap, repeat, or overflow | fail closed. |
| semantic duplicate at a later sequence with identical identity and stage payload | idempotent. |
| identity-changing duplicate or illegal stage order | fail closed. |
| selected source log ends with acknowledged/sealed unmatched `GenerationPrepared` | keep publication fenced; finish that exact generation or append/acknowledge/seal the immediately following `GenerationAborted`. |
| selected marker's anchor is valid but its required `MarkerCommitted` is absent | recover only through the explicit switch table above; a byte mismatch, bytes beyond the deterministic witness frame, or any ordinary/unknown intervening frame fails closed. |
| corruption before the unacknowledged suffix, or unexplained bytes after damage | fail closed. |
| unknown critical record/version or unsupported sidecar version | fail closed. |
| unknown noncritical record with valid bounds/CRCs/sequence | skip; v1 never emits one. |
| marker selects a missing/invalid snapshot or log | accepted safety-preserving availability failure: fail closed and never fall back from that valid marker slot. |

### 9.1 Tail repair

Only bytes proven to be beyond the last acknowledged seal are repairable. Syntactic frame completeness alone never decides acknowledgement. Under the exclusive lock:

1. Replay the sealed prefix through sequence `S` and acknowledgement epoch `A`; its length must equal the authoritative slot's `sealed_log_len`. Compute and record the CRC-32 of that exact prefix and the CRC-32 of the complete unacknowledged suffix.
2. Exclusive-create `retirement.log.g{G:020}.tail.o{O:020}.l{L:020}.c{C:08x}.bin`, where `O` is the sealed prefix length, `L` the suffix length, and `C` the suffix CRC-32, then copy/sync the exact suffix. Reuse an existing file only after its name fields, byte length, CRC-32, and full byte-for-byte content all match; mismatch is fatal. Do not modify the predecessor log.
3. Build generation `G + 1` snapshot state through `S`, with tail-repair flag and predecessor `G`.
4. Create generation `G + 1` log whose only bytes are one unsealed `LogOpened` at sequence `S + 1`, `open_reason = 1`, `predecessor_ack_epoch = A`, and the exact predecessor prefix length/CRC and suffix length/CRC.
5. Sync and publish both new files, then perform the applicable parent-directory durability step. Reopen and byte-verify both final names.
6. Write/sync/re-read the inactive marker slot selecting the new pair and its complete `LogOpened` frame as anchor.
7. Acknowledge/seal the existing `LogOpened` with an `ACTIVATED` acknowledgement slot bound to the new marker epoch/generation. Then append/acknowledge/seal `MarkerCommitted` at sequence `S + 2`.
8. Replay the selected pair. Only then permit ordinary writes at sequence `S + 3`.

If any step fails, the old pair remains immutable evidence or startup fails closed. No byte at or before the authoritative sealed watermark is repairable. Uncertainty about acknowledgement slots/seals cannot use this procedure, and the evidence name prevents a same-offset/different-length suffix from being reused accidentally.

## 10. Generation selection and compaction

The numeric maximum generation is never selected by itself. The selected marker slot is the commit point. Unreferenced higher generations are incomplete/orphan compaction output and are retained until audited; they never override the marker.

Compaction becomes a candidate when the active log is at least 64 MiB or contains at least 100,000 `Completed` records. These are scheduling thresholds, not permission to compact. Before starting, free space must cover the predicted new snapshot and log, their temporary copies, 64 MiB safety margin, and the two currently marker-referenced pairs. Insufficient space leaves the current append-only generation active and raises backpressure.

Compaction protocol from generation `G` to `G + 1`:

1. Stop new ledger appends at a bounded service barrier; active identities remain strongly owned.
2. Append/acknowledge/seal `GenerationPrepared` at sequence `S`, naming `G + 1`, and record its acknowledgement epoch `A`. The source log is now fenced at its exact authoritative sealed length.
3. Encode a canonical snapshot of state through `S`. Preserve every Allocated/Bound/Published incarnation still active or referenced, every incomplete ticket, every stage-5 Completed-retained ticket not made omission-eligible by a later clean-start revalidation, quarantine entries, and create/ticket high-water marks. A Completed ticket may be omitted only after the section 7 clean-start condition; completion and compaction in the same process never qualify.
4. Write the snapshot temporary file, sync it, and publish the final generation name without replacement.
5. Create the new log with exactly one **unsealed** `LogOpened` frame at sequence `S + 1`. It records predecessor acknowledgement epoch `A`, predecessor terminal sequence `S`, and the exact acknowledged source-prefix length/CRC; sync it and publish its final name. It is a candidate, not yet an acknowledged frame.
6. On Unix, sync the lifecycle directory. On Windows, use the sidecar-publication and external-fence procedure in section 11 without claiming POSIX directory durability. Reopen and byte-verify both final generation files.
7. Write the inactive marker slot with epoch + 1, the new snapshot length/CRC, and the complete `LogOpened` frame CRC as anchor; sync and re-read the marker byte-for-byte.
8. Run the section 8 protocol on the existing `LogOpened`: write acknowledgement epoch `A + 1` with `ACTIVATED`, the new marker epoch and generation; append/sync/re-read its seal. No other frame is appended before this completes.
9. Append/acknowledge/seal `MarkerCommitted` at sequence `S + 2` and acknowledgement epoch `A + 2`, binding the re-read marker slot epoch/index/stored CRC.
10. Replay the selected pair. Only then switch the ordinary writer to the new log, release the barrier, and use sequence `S + 3` / acknowledgement epoch `A + 3` for the next frame.

An acknowledged/sealed `GenerationPrepared` is a durable append fence: no ordinary lifecycle frame may follow it in the source generation. Recovery must either finish the exact named generation or append/acknowledge/seal `GenerationAborted` as sequence `S + 1` before resuming the source log. If marker update returns an ambiguous error, recovery first reopens both marker and acknowledgement slots. It may abort only when no valid marker selects the target and no acknowledgement slot or seal names the target generation/marker epoch. A valid newer marker is authoritative; a target acknowledgement/seal with a missing or invalid marker fails closed. Orphan target files are retained until the selected source state and their exact generation/CRC are audited; removing them never uses recursive deletion.

The older marker slot and its referenced pair remain a rollback copy. A generation pair may be removed only when neither valid marker slot nor valid acknowledgement slot references it, both current acknowledgement slots can be reconstructed from seals in retained logs, the current pair has passed a later clean-start replay, and exact filenames are deleted individually. Failure to remove old sidecars is harmless backlog. Recursive deletion is forbidden.

The generation-switch crash points are deterministic: before step 7 the old marker remains selected and the target contains at most an unsealed `LogOpened`; after step 7 the new marker is selected and recovery must finish that anchor's acknowledgement/seal; after step 8 it must append the witness; during either acknowledgement it follows the exact frame/seal tail table in section 9. No application frame exists in the new log before the witness. A valid newer marker whose pair is unavailable or invalid fails closed instead of losing potentially acknowledged retirement records.

### 10.1 Invalid-slot and higher-generation rule

An older valid marker slot is never a sufficient fallback decision. Before selecting it, startup enumerates every exact snapshot/log generation name above that slot's generation, both acknowledgement slots, and all syntactically reachable final seals, without following links. It classifies the complete set as follows:

- A higher generation is provably uncommitted only when its snapshot and log validate; the log is exactly one complete, valid, **unsealed** `LogOpened`; the selected source log ends in the matching acknowledged/sealed unmatched `GenerationPrepared` or has the exact section 9.1 tail evidence; neither acknowledgement slot nor any seal names the candidate generation; and no partial following bytes, unexplained file, missing half-pair, or generation gap exists.
- If the newer marker slot is valid, it is authoritative even while the acknowledgement still names the predecessor. The candidate must match the second switch-table row in section 9, after which recovery acknowledges/seals `LogOpened` and writes the witness.
- If a newer marker slot is invalid nonzero but an acknowledgement slot or valid seal names its epoch/generation, or if the higher log contains a sealed `LogOpened`, sealed `MarkerCommitted`, or any later frame, startup fails closed. Those bytes prove that the candidate cannot be treated as never selected/never acknowledged.
- If an acknowledgement slot is invalid nonzero, recover it only from an exact valid seal under section 5.3. Failure to reconstruct is fatal even when an older marker and acknowledgement slot are valid.
- Any corrupt candidate frame, partial seal, ordinary/unknown frame, missing snapshot/log half, generation gap, or inability to prove the complete relation is ambiguous and fails closed. A partial candidate file still bearing a `.tmp` name is retained but is not a final-generation candidate.
- A completely classified uncommitted compaction candidate may be resumed exactly or abandoned by appending/acknowledging/sealing `GenerationAborted` immediately after the source `GenerationPrepared`. A tail-repair candidate must be resumed exactly because its source log cannot accept another frame. Exact candidate files remain evidence until abort or repair selection is durable.

This scan also applies when filesystem metadata later damages a previously valid marker or acknowledgement slot. It prevents a complete, frame-aligned acknowledged suffix from disappearing behind an older valid slot. The implementation may provide an offline forensic recovery tool, but online replay never guesses that the older slot/watermark is current. Coordinated rollback to older CRC-valid contents across all marker, acknowledgement, and log copies is outside the accidental-corruption model in section 1.

## 11. Platform namespace and durability model

### 11.1 Unix

All operations use directory-relative, no-follow APIs and verify `fstat` identity on the opened handle. After durable intent and logical handoff, v1 may rename to the unique same-directory tombstone and unlink it, or directly unlink the canonical path. After rename/unlink, it calls `fsync` on the containing directory before appending the corresponding stage. A directory-sync error keeps the prior durable stage and retries/reconciles; it is not reported as durable namespace success. Existing mappings and open file descriptions may remain usable until their Rust owners drop.

### 11.2 Windows

Windows v1 uses handle-based identity and namespace changes:

1. Open the parent with `FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE`, share flags `FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE`, disposition `OPEN_EXISTING`, and `FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT`. Open the target with `DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE`, the same three share flags, `OPEN_EXISTING`, and `FILE_FLAG_OPEN_REPARSE_POINT`. Reject reparse points and escape from the reserved Store root.
2. Obtain `FILE_ID_INFO` with `GetFileInformationByHandleEx` and compare the serialized volume/file id before every rename or disposition operation.
3. Rename the verified source handle with `SetFileInformationByHandle(FileRenameInfoEx)` and `FILE_RENAME_INFO.Flags = 0`: no replace, same-directory target relative to the verified parent. If `FileRenameInfoEx` is unsupported, `FileRenameInfo` with `ReplaceIfExists = FALSE` is the only v1 fallback. Path-based replace is not a fallback.
4. Final deletion uses the verified tombstone handle with `SetFileInformationByHandle(FileDispositionInfoEx)` and exactly `FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS`. v1 does not set `IGNORE_READONLY_ATTRIBUTE`. If the extended class is unsupported, `FileDispositionInfo(DeleteFile = TRUE)` on the same verified handle is allowed. There is no path-based delete fallback.
5. Close and reopen the relevant names without following reparse points. Record `NamespaceAbsent` only when the targeted canonical incarnation and all authorized tombstones are absent or the canonical path is positively bound to a different incarnation.

The v1 error map is deliberately conservative:

| Result | Classification |
|---|---|
| API success, followed by exact handle/path/key verification | observed success; eligible for the next stage. |
| `ERROR_SHARING_VIOLATION`, `ERROR_LOCK_VIOLATION`, or `ERROR_DELETE_PENDING` | retryable pending with bounded backoff. |
| `ERROR_FILE_NOT_FOUND` or `ERROR_PATH_NOT_FOUND` | request reconciliation; never sufficient by itself for absence proof. |
| `ERROR_INVALID_PARAMETER` or `ERROR_NOT_SUPPORTED` on an Ex information class | use only the explicitly specified same-handle fallback, once; otherwise unsupported platform/fail closed. |
| `ERROR_ACCESS_DENIED` / `STATUS_CANNOT_DELETE` | retain the ticket and retry within policy; after the bounded transient window, degrade and require operator action. Never convert to success. |
| cross-volume, invalid-name, reparse, root-escape, key mismatch, or unexpected target-exists result | unverified/superseded or fatal policy violation; retain identity and do not mutate the target. |
| every other error | retain identity, mark the namespace attempt failed, and surface the exact stage/code; no optimistic success. |

Rename success means the API returned success, the source handle still reports the expected key, the unique tombstone reopens with that key, and the canonical name is absent or belongs to a positively verified replacement. Delete success means disposition succeeded, the handle was closed, and subsequent no-follow opens prove the targeted canonical identity and every authorized tombstone absent. These definitions, not the raw API return alone, control stage emission.

`FlushFileBuffers`/Rust `sync_all` is required for lifecycle file contents. Sidecar temporary-to-final publication uses a verified source handle and the retained verified parent handle with no replacement. The signed NTFS implementation uses `NtSetInformationFile(FileRenameInformationEx)` and falls back on the same source handle to `FileRenameInformation` with `ReplaceIfExists=false` only when the extended information class is unsupported. An implementation may instead qualify `SetFileInformationByHandle(FileRenameInfoEx)` with the equivalent `FILE_RENAME_INFO` contract, but it must pass its own signed OS/filesystem matrix before activation. Every successful rename is followed by final-name reopen and byte verification. This specification does **not** claim that Windows supplies a POSIX-equivalent directory `fsync`, nor that `FlushFileBuffers`, a successful rename, or a successful reopen alone makes a directory-entry transition power-loss durable. During initial bootstrap, `ACKNOWLEDGED.v1` and `ENABLED.v1` are therefore exclusive-created at their final names, fixed to 208 bytes, populated positionally, flushed, and re-read; a partial final file is a fenced resumable/fatal bootstrap artifact, never an absent marker signal.

Windows Wave B additionally requires a durable external activation-attempt fence outside the Store filesystem failure domain. Before the first local lifecycle mutation, operations persist an irreversible `activation_attempted` decision keyed by the canonical Store root and deployment identity, then enforce ACL/service policy that denies every pre-Wave-A binary access to that root. The fence is never cleared by local rollback and must remain effective if the lifecycle directory, marker, or rename disappears after power loss. If the control plane cannot provide and verify this fence, Windows Wave B is a No-Go. This is an operational safety prerequisite, not an extra on-disk v1 field.

Durable `RetirementIntent` remains authoritative; `Tombstoned` and `NamespaceAbsent` are durable observations that startup must revalidate. Windows replay also revalidates every stage-5 Completed-retained ticket using its preserved canonical path, physical key, and optional tombstone path before marking it eligible for omission by a later compaction. If a recorded rename is not present after restart, reconciliation repeats the identity-checked operation or remains pending. A valid marker selecting missing generation files, or a marker/acknowledgement surviving while a selected final generation name disappears, is an accepted safety-preserving availability failure: fail closed, retain artifacts, and never use legacy fallback.

Wave B is allowed only on Windows filesystems and OS builds covered by the signed durability/platform test matrix. Local NTFS is the initial required target. ReFS, SMB, CSVFS, and other providers require their own evidence; successful compilation is not evidence of equivalent semantics.

The relevant Microsoft API contracts are:

- [FILE_ID_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info)
- [FILE_RENAME_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_rename_info)
- [SetFileInformationByHandle](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-setfileinformationbyhandle)
- [FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
- [FILE_DISPOSITION_INFORMATION_EX flags and semantics](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_disposition_information_ex)

## 12. Wave A, Wave B, and bootstrap

### 12.1 Wave A: compatible reader/fencing, writes disabled

Every binary that might open the Store is deployed with v1 marker/recognized-artifact detection, bounds checking, fail-closed unknown-version handling, and startup fencing. `managed_lifecycle_write_enabled` remains false. Wave A binaries MUST NOT create the lifecycle directory or marker, bootstrap data, issue tokens, or perform capability-bearing queue removal.

The marker cannot fence an older binary that does not know to read it. Operations must inventory and isolate every old broker, tool, backup job, and maintenance process using deployment policy, filesystem ACLs, and the Store exclusive lock. They also provision and test the external activation-attempt fence required above. An incomplete inventory or ineffective fence is Wave B No-Go.

### 12.2 Wave B: explicit activation

Under a maintenance window and exclusive Store lock:

1. Verify signed format/golden/platform evidence, the Wave A inventory, and the tested external activation-attempt/ACL fence. Persist that irreversible external fence before any local mutation.
2. Block allocator, queue publication, and lifecycle maintenance.
3. Create `.rocketmq-lifecycle`, then make the directory entry durable where the platform supports it.
4. Generate UUID/bootstrap id. Write, sync, and publish `store.meta`; verify it by reopening.
5. Exclusive-create final-name `ACKNOWLEDGED.v1` as 208 zero bytes, sync it, and re-read its exact length/content before creating a log.
6. Create generation-0 log. Append `StoreInitialized` at sequence 1, then use acknowledgement epoch 1 / slot 0 / flags 0 / marker epoch 0 to acknowledge and seal it.
7. Scan existing numeric segments with no-follow handles; verify root containment, ordering, size, and physical keys; assign create sequences without publishing anything new.
8. Write, sync, publish, and reopen-verify the bootstrap snapshot at base sequence 1 with all active incarnations and high-water marks.
9. Append `BootstrapInstalled` at sequence 2, binding the snapshot length and complete-file CRC; acknowledge/seal it with epoch 2 / slot 1 / flags 0 / marker epoch 0.
10. Replay meta + snapshot + the two sealed units and reconcile the live directory.
11. Create `ENABLED.v1` at exactly 208 bytes with valid slot 0 (epoch 1, generation 0, anchor sequence 2 and the complete `BootstrapInstalled` frame CRC) and zero slot 1. On Unix, a synced temporary file may be published without replacement followed by directory `fsync`; on Windows, exclusive-create the final name and populate it positionally as specified in section 11. Sync/flush and reopen/re-read the exact marker bytes.
12. Append `MarkerCommitted` at sequence 3; acknowledge/seal it with epoch 3 / slot 0 / `ACTIVATED` / marker epoch 1, overwriting the initial acknowledgement slot. Replay/reconcile a second time. Only then allow queue publication and capability issuance; the next ordinary frame uses sequence 4 / acknowledgement epoch 4 / slot 1.

Before step 12 completes, no durable delete capability can have been issued. A compatible bootstrap tool may resume only the exact next step when UUID/bootstrap id, acknowledgement epochs/seals, final generation bytes, inventory, and marker state all agree. Otherwise it retains exact artifacts and fails closed; restarting with a new directory requires explicit offline operator direction and does not clear the external fence. It never recursively clears the lifecycle directory.

Any recognized lifecycle artifact (`store.meta`, acknowledgement file, generation file, lifecycle directory, or external fence) without a valid marker disables writable legacy startup. Pre-activation artifacts may enter only the exact bootstrap-resume states above. `ACTIVATED` acknowledgement/seal evidence without the marker, or `ENABLED.v1` with missing meta/acknowledgement/pair/anchor/reconciliation proof, is fatal until forensic recovery.

## 13. Rollback, downgrade, backup, and restore

- During Wave A, code can roll back normally. After the external activation-attempt fence is set, even if local bootstrap has not reached `MarkerCommitted`, only a compatible v1 binary may access or resume the Store; no capability was issued, but the fence is intentionally irreversible.
- After marker activation, only a binary that understands and fences v1 may open the Store. A compatible binary may run read-only recovery, but an older binary must be denied filesystem access.
- In-place deletion, renaming, zeroing, or ignoring of `ENABLED.v1` or `ACKNOWLEDGED.v1` is forbidden. Restoring only one sidecar file or clearing the external fence is forbidden.
- Code rollback after activation must retain v1 replay/reconciliation and must not issue new capabilities if its writer behavior is not identical.
- Offline downgrade is either a restore of a complete pre-activation backup, or an audited export into a **new** Store root. Export requires successful v1 replay, zero incomplete tickets/tombstones, no unknown/quarantined namespace entries, and exact verification of every canonical file. It copies data; it does not modify the source or remove its marker.
- A backup is consistent only when taken under the Store lock or from a filesystem snapshot that atomically includes Store data, `store.meta`, both `ENABLED.v1` slots, both `ACKNOWLEDGED.v1` slots, and every marker/acknowledgement-referenced generation pair. Restore operations must also preserve/re-establish the external activation fence before exposing the root.
- Restore to a new machine/filesystem commonly changes physical keys. The Store remains fenced until an explicit offline rebind tool verifies every file's bytes/length/path and writes a separately reviewed migration generation. Silent rebind is forbidden.

There is no downgrade path that treats `NotFound`, a missing marker, or an empty log as proof that pending deletion rights never existed.

## 14. Golden fixtures and decoder acceptance

Before Wave B, `rocketmq-store-local/tests/fixtures/mapped_file_lifecycle/` must contain binary fixtures and a checked-in SHA-256 manifest for:

- `store.meta`, both marker-slot choices, both acknowledgement-slot choices, bootstrap snapshot, compacted snapshot, and tail-repair snapshot;
- every record type in section 6, including Unix and Windows physical keys and maximum-length paths;
- complete frame/seal units, seal CRC/slot-CRC mismatch, valid acknowledgement with missing/partial seal repair, torn nonzero acknowledgement, exact seal-based slot reconstruction, and forbidden nonconsecutive/all-zero slot histories;
- each legal direct and tombstone retirement chain;
- stage-5 Completed-retained snapshots before clean-start revalidation and omission only by a later compaction after successful exact path/key/tombstone revalidation;
- unacknowledged suffix at every frame/seal boundary, acknowledged frame truncation at every field, complete frame-aligned acknowledged suffix loss, and overlong suffix;
- bad header CRC, bad payload CRC, full corrupt final frame, mid-log damage, sequence gap/duplicate/overflow, oversized header/payload/snapshot, invalid UTF-8/path/enum/reserved bits;
- unknown critical and valid unknown noncritical records;
- old-slot/new-slot selection, torn newer marker, valid newer marker with missing pair, orphan higher generation, and every compaction/tail-repair point before marker switch, between marker and `LogOpened` seal, and before/after `MarkerCommitted` seal;
- same-path replacement with a different physical key and a unique damaged `RetirementIntent`.

Fixtures are immutable compatibility artifacts. Updating a byte or expected hash is an on-disk format change requiring format-owner review; tests must compare encoder output byte-for-byte and decoder/replay outcome, not just round-trip values.

## 15. Complete worked acknowledged unit

This vector uses a 100-byte `Completed` frame followed by its 72-byte commit seal. The frame uses sequence 100, log generation 2, ticket 42, UUID `000102030405060708090a0b0c0d0e0f`, create sequence 7, completion time `0x0102030405060708`, prerequisite `NamespaceAbsent` sequence 9, and proof flags 3. The corresponding acknowledgement uses epoch 77, slot 0, `ACTIVATED`, marker epoch 5, bootstrap id `101112131415161718191a1b1c1d1e1f`, frame end 100, and sealed length 172.

The exact 104-byte populated acknowledgement slot is:

```text
0000: 52 4d 41 43 01 00 00 00 68 00 00 01 00 00 00 00
0010: 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
0020: 10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f
0030: 4d 00 00 00 00 00 00 00 05 00 00 00 00 00 00 00
0040: 02 00 00 00 00 00 00 00 64 00 00 00 00 00 00 00
0050: 64 00 00 00 00 00 00 00 22 35 db 50 ac 00 00 00
0060: 00 00 00 00 3b 6b 90 bb
```

Its stored slot CRC is `0xBB906B3B`, serialized `3b 6b 90 bb`, over slot bytes `[0x0000,0x0064)`. The SHA-256 of the complete slot is `e019edc5aa28c9ac8bdc29c20108fcff6ddf5c6adc2fa21ba78b66670b620ea7`.

The exact 172-byte log unit (frame at `[0,100)`, seal at `[100,172)`) is:

```text
0000: 52 4d 4c 43 01 00 00 00 24 00 01 00 01 00 28 00
0010: 38 00 00 00 64 00 00 00 00 00 00 00 02 00 00 00
0020: 00 00 00 00 04 d8 d0 0f 2a 00 00 00 00 00 00 00
0030: 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
0040: 07 00 00 00 00 00 00 00 08 07 06 05 04 03 02 01
0050: 09 00 00 00 00 00 00 00 03 00 00 00 00 00 00 00
0060: 19 fc ea 0a 52 4d 43 53 01 00 00 00 48 00 00 01
0070: 00 00 00 00 4d 00 00 00 00 00 00 00 05 00 00 00
0080: 00 00 00 00 02 00 00 00 00 00 00 00 64 00 00 00
0090: 00 00 00 00 64 00 00 00 00 00 00 00 22 35 db 50
00a0: 3b 6b 90 bb 00 00 00 00 ef 9d c0 c9
```

The frame header CRC is `0x0FD0D804`, serialized `04 d8 d0 0f`, over bytes `[0x0000,0x0024)`. The payload CRC is `0x0AEAFC19`, serialized `19 fc ea 0a`, over the 56 payload bytes `[0x0028,0x0060)`. The CRC-32 of the complete frame, including both stored CRC fields, is `0x50DB3522`, serialized into the acknowledgement slot and seal as `22 35 db 50`. The seal CRC is `0xC9C09DEF`, serialized `ef 9d c0 c9`, over seal-relative bytes `[0x00,0x44)` (log bytes `[0x64,0xa8)`). The stored acknowledgement-slot CRC inside the seal is `0xBB906B3B`.

The SHA-256 values are:

| Bytes | Length | SHA-256 |
|---|---:|---|
| frame | 100 | `38b8ae1b8279222529f89270c7ffc5853146940c4abd91c24a6f3af1990af2b1` |
| seal | 72 | `e283908bed5b95d58359d9691ce7a49b722b8e21f22ba7eac31a5419ee5adc13` |
| frame + seal | 172 | `5c243cc25589cb54c89c49afc796e6e41a12a19604a8eecf0242776797e5bdf2` |

Implementations should also assert the CRC check vector `CRC32("123456789") == 0xCBF43926` before comparing this fixture.

## 16. Implementation sign-off checklist

- [ ] All fixed offsets, record numbers, enums, limits, CRC parameters, and path rules are represented by byte-for-byte golden tests.
- [ ] Decode validates limits before allocation and never uses path-only authorization.
- [ ] Marker and acknowledgement two-slot selection, seal reconstruction, `MarkerCommitted`, higher-generation scan, and every old-slot/watermark fallback ambiguity are fault-injected.
- [ ] Every write/sync/rename/unlink/disposition/marker-switch point is covered by crash replay.
- [ ] Tail repair accepts only a suffix proven unacknowledged by slots/seals, preserves exact length/CRC/bytes as evidence, and never truncates the predecessor.
- [ ] Compaction preserves active/unpublished incarnations, incomplete tickets, non-eligible Completed-retained tickets, quarantine state, and identifier high waters.
- [ ] Wave A inventory/fencing evidence and Wave B activation record are signed.
- [ ] Native Linux and Windows tests record OS build and actual filesystem; Windows does not claim POSIX directory durability.
- [ ] Bootstrap, compatible rollback, offline export, backup, and explicit rebind procedures are rehearsed.
- [ ] Reconciliation completes before any numeric segment publication or durable capability issuance.
