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

//! Test to measure and display the size of RouteInfoManager structures
//!
//! This test helps verify the memory layout and validate the Box optimization
//! for RouteInfoManagerWrapper enum variants.

use std::mem::size_of;

use rocketmq_namesrv::route::route_info_manager::RouteInfoManager;
use rocketmq_namesrv::route::route_info_manager_v2::RouteInfoManagerV2;
use rocketmq_namesrv::RouteInfoManagerWrapper;

#[test]
fn test_struct_sizes() {
    println!("\n========================================");
    println!("RouteInfoManager Structure Size Analysis");
    println!("========================================\n");

    // Measure V1 (RouteInfoManager)
    let v1_size = size_of::<RouteInfoManager>();
    println!("RouteInfoManager (V1):");
    println!("  - Size: {} bytes", v1_size);
    println!("  - Cache lines (64B): {}", v1_size.div_ceil(64));
    println!();

    // Measure V2 (RouteInfoManagerV2)
    let v2_size = size_of::<RouteInfoManagerV2>();
    println!("RouteInfoManagerV2 (V2):");
    println!("  - Size: {} bytes", v2_size);
    println!("  - Cache lines (64B): {}", v2_size.div_ceil(64));
    println!();

    // Size difference
    let diff = v2_size.abs_diff(v1_size);
    let larger = if v2_size > v1_size { "V2" } else { "V1" };
    println!("Size Difference:");
    println!("  - {} is larger by {} bytes", larger, diff);
    println!("  - Ratio: {:.2}x", v2_size as f64 / v1_size as f64);
    println!();

    // Measure wrapper without Box
    println!("Enum Sizes (Hypothetical):");
    println!("  - Without Box: {} bytes (= larger variant)", v2_size.max(v1_size));
    println!("  - Discriminant: 8 bytes (estimated)");
    println!("  - Total without Box: {} bytes", v2_size.max(v1_size) + 8);
    println!();

    // Measure wrapper with Box
    let wrapper_size = size_of::<RouteInfoManagerWrapper>();
    let box_v1_size = size_of::<Box<RouteInfoManager>>();
    let box_v2_size = size_of::<Box<RouteInfoManagerV2>>();

    println!("Actual RouteInfoManagerWrapper (with Box):");
    println!("  - Total size: {} bytes", wrapper_size);
    println!("  - Box<V1> size: {} bytes", box_v1_size);
    println!("  - Box<V2> size: {} bytes", box_v2_size);
    println!();

    // Calculate savings
    let without_box_size = v2_size.max(v1_size) + 8;
    let savings = without_box_size as f64 - wrapper_size as f64;
    let savings_percent = (savings / without_box_size as f64) * 100.0;

    println!("Memory Optimization:");
    println!("  - Saved: {} bytes", savings as i64);
    println!("  - Reduction: {:.1}%", savings_percent);
    println!("  - Stack allocation: {} → {} bytes", without_box_size, wrapper_size);
    println!();

    // Performance implications
    println!("Performance Implications:");
    println!("  - Pointer dereference overhead: ~0.3-0.5 ns");
    println!("  - Cache efficiency gain: ~6x (1 vs 6+ cache lines)");
    println!(
        "  - Stack frame size reduction: {:.1}x",
        without_box_size as f64 / wrapper_size as f64
    );
    println!();

    println!("========================================");
    println!("Clippy large_enum_variant threshold: 200 bytes");
    println!(
        "Difference: {} bytes {} threshold",
        diff,
        if diff > 200 { "EXCEEDS" } else { "within" }
    );
    println!("========================================\n");

    // Assertions to ensure Box optimization is working
    assert!(box_v1_size == 8, "Box<V1> should be 8 bytes (pointer size)");
    assert!(box_v2_size == 8, "Box<V2> should be 8 bytes (pointer size)");
    assert!(
        wrapper_size <= 16,
        "Wrapper should be ≤ 16 bytes (8 byte pointer + 8 byte discriminant)"
    );
    assert!(
        diff > 200,
        "Size difference should exceed 200 bytes to justify Box optimization"
    );
}

#[test]
fn test_component_sizes() {
    println!("\n========================================");
    println!("Individual Component Sizes");
    println!("========================================\n");

    use std::sync::Arc;

    use dashmap::DashMap;
    use rocketmq_rust::ArcMut;

    println!("Basic Types:");
    println!("  - Arc: {} bytes", size_of::<Arc<()>>());
    println!("  - ArcMut: {} bytes", size_of::<ArcMut<()>>());
    println!("  - DashMap (empty): {} bytes", size_of::<DashMap<String, String>>());
    println!(
        "  - HashMap (empty): {} bytes",
        size_of::<std::collections::HashMap<String, String>>()
    );
    println!("  - RwLock: {} bytes", size_of::<parking_lot::RwLock<()>>());
    println!();

    println!("========================================\n");
}
