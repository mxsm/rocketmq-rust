// Copyright 2025 The RocketMQ Rust Authors
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(windows)]
    {
        println!("cargo:rerun-if-changed=resources/windows/app.rc");
        println!("cargo:rerun-if-changed=assets/app-icon.ico");
        embed_resource::compile_for(
            "resources/windows/app.rc",
            ["rocketmq-dashboard-gpui"],
            embed_resource::NONE,
        )
        .manifest_required()?;
    }
    Ok(())
}
