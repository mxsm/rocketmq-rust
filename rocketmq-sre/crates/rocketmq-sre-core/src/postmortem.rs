// Copyright 2026 The RocketMQ Rust Authors
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

//! Deterministic postmortem assembly, rendering, and lifecycle validation.

mod assembler;
mod template;
mod validation;

pub use assembler::PostmortemActionProposal;
pub use assembler::PostmortemAssembly;
pub use assembler::PostmortemAssemblyInput;
pub use assembler::assemble;
pub use template::render_markdown;
pub use validation::PostmortemValidationError;
pub use validation::validate_action_item_transition;
pub use validation::validate_revision;
