// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Controller CLI Usage Example
//!
//! This example demonstrates how to use the Controller CLI argument parsing
//! in a custom application.
//!
//! # Run this example
//!
//! ```bash
//! # Parse command line with config file
//! cargo run --example cli_usage -- -c examples/controller.toml
//!
//! # Print configuration
//! cargo run --example cli_usage -- -c examples/controller.toml -p
//!
//! # Show help
//! cargo run --example cli_usage -- --help
//! ```

use rocketmq_controller::parse_command_line;
use rocketmq_controller::ControllerCli;
use rocketmq_error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== RocketMQ Controller CLI Usage Example ===\n");

    // Method 1: Use high-level parse_command_line() function
    println!("Method 1: Using parse_command_line()");
    let (cli, config) = parse_command_line()?;

    println!(" Configuration loaded successfully!\n");

    // Display CLI arguments
    println!("CLI Arguments:");
    println!("  Config File: {:?}", cli.config_file);
    println!("  Print Mode:  {}", cli.print_config_item);
    println!();

    // Display loaded configuration
    println!("Configuration Summary:");
    println!("  RocketMQ Home: {}", config.rocketmq_home);
    println!("  Node ID:       {}", config.node_id);
    println!("  Listen Addr:   {}", config.listen_addr);
    println!("  Raft Timeout:  {} ms", config.election_timeout_ms);
    println!("  Heartbeat:     {} ms", config.heartbeat_interval_ms);
    println!();

    // Method 2: Manual parsing (more control)
    println!("Method 2: Manual parsing for more control");

    // Parse arguments
    let cli_manual = ControllerCli::parse_args();

    // Validate
    cli_manual.validate()?;
    println!("Arguments validated");

    // Load config
    let default_config = rocketmq_controller::ControllerConfig::default();
    let config_manual = cli_manual.load_config(default_config)?;
    println!(" Configuration loaded");
    println!();

    // Print detailed configuration if requested
    if cli_manual.print_config_item {
        ControllerCli::print_config(&config_manual);
    } else {
        println!("Configuration loaded. Use -p to print full configuration.");
    }

    // Demonstrate usage with ControllerManager
    println!("\n=== Next Steps ===");
    println!("To start the controller:");
    println!("1. Create ControllerManager with config");
    println!("2. Initialize the manager");
    println!("3. Start the controller");
    println!("\nExample code:");
    println!(
        r#"
    use rocketmq_controller::{{startup::parse_command_line, ControllerManager}};
    
    #[tokio::main]
    async fn main() -> Result<()> {{
        let (_cli, config) = parse_command_line()?;
        
        let mut controller = ControllerManager::new(config).await?;
        controller.initialize().await?;
        controller.start().await?;
        
        // Controller is now running...
        
        Ok(())
    }}
    "#
    );

    Ok(())
}
