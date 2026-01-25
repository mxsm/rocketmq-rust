---
sidebar_position: 1
title: Overview
---

# Contributing Overview

Welcome to the RocketMQ-Rust community! 🎉

Thank you for your interest in contributing to RocketMQ-Rust. Whether you're fixing a bug, adding a feature, or improving documentation, every contribution makes a difference. This guide will help you get started on your contribution journey.

## Ways to Contribute

There are many ways to contribute to RocketMQ-Rust:

### Code Contributions

- **Bug fixes**: Fix reported issues
- **New features**: Add new functionality
- **Performance improvements**: Optimize existing code
- **Documentation**: Improve code documentation
- **Tests**: Add unit and integration tests

### Non-Code Contributions

- **Bug reports**: Report bugs and issues
- **Feature requests**: Suggest new features
- **Documentation**: Improve user documentation
- **Code review**: Review pull requests
- **Community support**: Help other users

## Getting Started

### 1. Fork and Clone

```bash
# Fork the repository on GitHub
# Clone your fork
git clone https://github.com/YOUR_USERNAME/rocketmq-rust.git
cd rocketmq-rust

# Add upstream remote
git remote add upstream https://github.com/mxsm/rocketmq-rust.git
```

### 2. Set Up Development Environment

```bash
# Install Rust nightly toolchain
rustup toolchain install nightly
rustup default nightly

# Install development tools
rustup component add rustfmt clippy

# Build the project
cargo build
```

### 3. Create a Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/your-feature-name
```

## Development Workflow

### Making Changes

1. **Write code**: Follow the coding standards
2. **Add tests**: Ensure test coverage
3. **Format code**: Use rustfmt
```bash
cargo fmt
```
4. **Run linter**: Use clippy
```bash
cargo clippy -- -D warnings
```
5. **Run tests**: Ensure all tests pass
```bash
cargo test --all
```

### Committing Changes

Use clear commit messages:

```
feat: Add transaction message support

- Implement TransactionProducer
- Add transaction listener trait
- Add unit tests for transaction messages

Closes #123
```

Commit message format:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test changes
- `refactor:` Code refactoring
- `perf:` Performance improvement
- `chore:` Build process or tooling

### Submitting Pull Request

1. **Push to your fork**:
```bash
git push origin feature/your-feature-name
```

2. **Create pull request**: On GitHub

3. **Fill PR template**:
   - Describe your changes
   - Reference related issues
   - Add screenshots if applicable

4. **Wait for review**: Maintainers will review your PR

## Code Review Process

### What to Expect

Our review process is designed to maintain code quality while being welcoming to contributors:

1. **Automated checks** (1-5 minutes): CI runs tests, linting, and format checks
2. **Manual review** (1-3 days): Maintainers review your code for quality and design
3. **Feedback discussion**: Collaborative discussion to improve the code
4. **Approval & merge**: Once approved, your PR will be merged! 🎉

### Addressing Feedback

Code review is a conversation, not criticism. When you receive feedback:

- Respond to all review comments (even if just to acknowledge)
- Ask questions if something is unclear
- Make requested changes and push updates to the same branch
- Request re-review when ready
- Don't hesitate to discuss alternative approaches

**Note**: CodeRabbit suggestions are helpful references, but the final decision is made by maintainers during code review.

## Coding Standards

See [Coding Standards](./coding-standards) for detailed guidelines.

### Key Principles

1. **Follow Rust idioms**: Write idiomatic Rust code
2. **Error handling**: Use `Result` types properly
3. **Documentation**: Add doc comments to public APIs
4. **Testing**: Write comprehensive tests
5. **Performance**: Consider performance implications

### Example

```rust
//! Producer module for sending messages to RocketMQ brokers.

use crate::error::{Error, Result};
use crate::model::Message;

/// A RocketMQ producer for sending messages.
///
/// # Examples
///
/// ```rust
/// use rocketmq::producer::Producer;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let producer = Producer::new();
/// producer.start().await?;
/// # Ok(())
/// # }
/// ```
pub struct Producer {
    // Implementation
}

impl Producer {
    /// Creates a new producer instance.
    pub fn new() -> Self {
        Self { /* ... */ }
    }

    /// Sends a message to the broker.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The broker is not available
    /// - The message exceeds maximum size
    /// - Network timeout occurs
    pub async fn send(&self, message: Message) -> Result<SendResult> {
        // Implementation
    }
}
```

## Development Guide

See [Development Guide](./development-guide) for detailed information.

## Testing

### Running Tests

```bash
# Run all tests
cargo test --all

# Run specific test
cargo test test_send_message

# Run tests with output
cargo test -- --nocapture

# Run tests in parallel
cargo test --all -- --test-threads=4
```

### Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_message() {
        let producer = Producer::new();
        producer.start().await.unwrap();

        let message = Message::new("TestTopic".to_string(), b"Test".to_vec());
        let result = producer.send(message).await;

        assert!(result.is_ok());
    }
}
```

## Documentation

### Building Documentation

```bash
# Build documentation
cargo doc --no-deps --open
```

### Writing Documentation

```rust
/// Summary (one sentence)
///
/// More detailed explanation.
///
/// # Examples
///
/// ```
/// use rocketmq::producer::Producer;
///
/// let producer = Producer::new();
/// ```
///
/// # Errors
///
/// This function will return an error if...
///
/// # Panics
///
/// This function will panic if...
pub fn public_function() -> Result<()> {
    // Implementation
}
```

## Getting Help

We're here to help! Feel free to reach out through:

- **GitHub Issues**: [Report bugs or request features](https://github.com/mxsm/rocketmq-rust/issues)
- **GitHub Discussions**: [Ask questions and share ideas](https://github.com/mxsm/rocketmq-rust/discussions)
- **Email**: [mxsm@apache.org](mailto:mxsm@apache.org)

## License

RocketMQ-Rust is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). By contributing, you agree that your contributions will be licensed under the same license.

## Code of Conduct

We are committed to providing a welcoming and inclusive environment for everyone. Please:

- Be respectful and considerate in your communication
- Welcome newcomers and help them get started
- Focus on constructive feedback
- Assume good intentions
- Collaborate openly and share knowledge

We're all here to build great software together! 🚀

## Next Steps

- [Development Guide](./development-guide) - Detailed development information
- [Coding Standards](./coding-standards) - Code style guidelines
- [Report Issues](https://github.com/mxsm/rocketmq-rust/issues) - File a bug or feature request
