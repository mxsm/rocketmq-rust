# RocketMQ Dashboard

A modern GUI dashboard for RocketMQ built with [gpui](https://github.com/zed-industries/zed), the GPU-accelerated UI framework used by the Zed editor.

## Features

- 🎨 **Apple-style Design**: Inspired by Apple's Human Interface Guidelines
- 🚀 **GPU-accelerated**: Built on gpui for smooth, responsive performance
- 📊 **Comprehensive Dashboard**:
  - Dashboard overview with metrics
  - Message management and querying
  - Topic configuration and monitoring
  - Consumer group management
  - Cluster and broker monitoring
- 🌗 **Theme Support**: Light and dark mode (Apple-inspired)
- 🎯 **Native Performance**: Rust-powered, cross-platform GUI

## Screenshots

*Coming soon*

## Installation

### Prerequisites

- Rust 1.85.0 or later
- Cargo

### Build from Source

```bash
cd rocketmq-dashboard
cargo build --release
```

### Run

```bash
cargo run
```

## Usage

### Basic Usage

1. Launch the dashboard:
   ```bash
   rocketmq-dashboard
   ```

2. Connect to your RocketMQ instance (configuration coming soon)

3. Navigate through the sidebar to explore:
   - **Dashboard**: Overview metrics and statistics
   - **Message**: View, query, and send messages
   - **Topic**: Manage topics and configurations
   - **Consumer**: Monitor consumer groups
   - **Cluster**: View cluster and broker status

## Configuration

Configuration files are stored in:
- **Linux/macOS**: `~/.config/rocketmq/dashboard/`
- **Windows**: `%APPDATA%\rocketmq\dashboard\`

Configuration options coming soon.

## Development

### Project Structure

```
rocketmq-dashboard/
├── src/
│   ├── main.rs           # Application entry point
│   ├── app.rs            # Main application logic
│   ├── theme.rs          # Apple-style theme definitions
│   ├── ui.rs             # UI module exports
│   └── ui/
│       ├── dashboard_view.rs  # Main dashboard view
│       ├── sidebar.rs         # Navigation sidebar
│       └── pages/
│           ├── dashboard.rs   # Dashboard overview page
│           ├── message.rs     # Message management page
│           ├── topic.rs       # Topic management page
│           ├── consumer.rs    # Consumer group page
│           └── cluster.rs     # Cluster monitoring page
├── Cargo.toml
└── README.md
```

### Adding New Features

1. Create a new page in `src/ui/pages/`
2. Implement the `Render` trait
3. Add navigation item to `sidebar.rs`
4. Export from `src/ui/pages.rs`

### Code Style

Follow Rust best practices:
- Use `rustfmt` for formatting
- Use `clippy` for linting
- Write tests for new functionality
- Document public APIs

## Roadmap

- [ ] Connect to RocketMQ instances
- [ ] Real-time metrics and charts
- [ ] Message sending interface
- [ ] Topic creation and configuration
- [ ] Consumer group management
- [ ] Dark mode toggle
- [ ] Settings and preferences
- [ ] Multiple instance support
- [ ] Export/import configurations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Licensed under either of
- Apache License, Version 2.0, ([LICENSE-APACHE](../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

- Built with [gpui](https://github.com/zed-industries/zed)
- Design inspired by [Apple's Human Interface Guidelines](https://developer.apple.com/design/human-interface-guidelines/)
- Part of the [RocketMQ Rust](https://github.com/mxsm/rocketmq-rust) project
