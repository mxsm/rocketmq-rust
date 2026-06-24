---
name: rust-doc-comment-generator
description: Generate idiomatic, production-grade Rust comments and documentation strictly following official Rustdoc and Rust API documentation conventions. Designed for real-world Rust projects with zero AI-identifiable markers.

---

# Rust Documentation & Comment Generator Skill

## Overview

This skill generates **standard-compliant Rust comments and documentation** for Rust source code.
It strictly follows Rust’s official documentation guidelines and produces output suitable for
**production-quality open-source and enterprise Rust projects**.

The generated comments are:

- Idiomatic and precise
- Free of AI-identifiable artifacts
- Fully aligned with Rustdoc conventions
- Suitable for blocking, async, and unsafe code

## Standards & References

This skill MUST comply with the following authoritative sources:

- Rust API Guidelines — Documentation  
  https://rust-lang.github.io/api-guidelines/documentation.html
- The Rustdoc Book  
  https://doc.rust-lang.org/rustdoc/
- Rust Style Guide  
  https://doc.rust-lang.org/style-guide/

## Supported Targets

This skill applies to the following Rust items:

- `struct`, `enum`, `union`
- `trait`
- `impl` blocks
- `fn` / `async fn`
- Modules (`mod`)
- `unsafe` blocks and functions

## Comment & Documentation Rules

### 1. Comment Types

| Context                       | Format                |
|-------------------------------|-----------------------|
| Public item                   | `///` Rustdoc comment |
| Module-level documentation    | `//!`                 |
| Private implementation detail | `//`                  |
| Unsafe API explanation        | `/// # Safety`        |

### 2. Rustdoc Section Usage

Rustdoc sections MUST be included **only when semantically relevant**:

- `# Examples`
- `# Panics`
- `# Errors`
- `# Safety`
- `# Performance`

Empty or boilerplate sections are not allowed.

### 3. Language & Tone

- Use formal, neutral, technical language
- Avoid conversational or instructional phrasing
- Avoid marketing or subjective language
- Describe behavior, constraints, and guarantees precisely

✅ Correct:

> Represents the configuration used by the message consumer.

❌ Incorrect:

> This struct is very useful and highly optimized.

### 4. Blocking vs Async Behavior

#### Blocking APIs

Blocking behavior MUST be explicitly documented.

```rust
/// Blocks the current thread until a message is available.
```

#### Async / Non-Blocking APIs

Async behavior MUST be explicitly documented.

```rust
/// Asynchronously waits for the next message.
///
/// This function does not block the calling thread.
```

### 5. Unsafe Code Documentation

Any `unsafe` function or block MUST include a `# Safety` section.

```rust
/// # Safety
///
/// The caller must ensure that the pointer is valid and properly aligned
/// for the duration of the call.
```

Vague or generic safety statements are forbidden.

## Forbidden Content

The output MUST NOT contain:

- Emojis
- Phase or workflow markers (e.g. `Phase`, `Step`, `Optimize`)
- TODO / FIXME / NOTE meta-comments
- AI-related indicators or explanations
- Commentary about refactoring or future improvements

## Input Expectations

The user may provide:

- Undocumented Rust code
- Partially documented Rust code
- Rust code with non-standard or low-quality comments

## Output Expectations

The skill MUST:

1. Preserve existing correct Rustdoc comments
2. Rewrite non-standard comments into idiomatic Rustdoc
3. Add missing documentation where appropriate
4. Never change code semantics
5. Never introduce new APIs or rename identifiers

## Example

### Input

```rust
pub struct MessageQueue {
    capacity: usize,
}
```

### Output

```rust
/// Represents a bounded message queue.
///
/// The queue can store messages up to a fixed capacity.
pub struct MessageQueue {
    capacity: usize,
}
```

## Non-Goals

This skill does NOT:

- Refactor code
- Optimize performance
- Rename symbols
- Add logging
- Generate tests

## Compatibility

This skill is designed to work alongside:

- Rust API naming validation skills
- Safety auditing skills
- Project-specific glossary enforcement skills

Each skill operates independently and does not overlap responsibilities.