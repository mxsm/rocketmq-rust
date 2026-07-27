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

use std::time::Duration;

use serde::Serialize;

/// Hard limits applied to every Phase 1 diagnosis.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrchestratorLimits {
    pub(crate) max_tool_calls: u8,
    pub(crate) max_query_retries: u8,
    pub(crate) max_schema_repairs: u8,
    pub(crate) max_evidence_items: u32,
    pub(crate) evidence_timeout: Duration,
}

impl Default for OrchestratorLimits {
    fn default() -> Self {
        Self {
            max_tool_calls: 12,
            max_query_retries: 2,
            max_schema_repairs: 1,
            max_evidence_items: 200,
            evidence_timeout: Duration::from_secs(15),
        }
    }
}

/// Immutable accounting included in every diagnosis response.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct BudgetUsage {
    pub(crate) tool_calls_used: u8,
    pub(crate) tool_calls_limit: u8,
    pub(crate) query_retries_used: u8,
    pub(crate) query_retries_limit: u8,
    pub(crate) schema_repairs_used: u8,
    pub(crate) schema_repairs_limit: u8,
    pub(crate) model_input_tokens: u32,
    pub(crate) model_output_tokens: u32,
}

impl BudgetUsage {
    pub(crate) const fn rules_only(limits: OrchestratorLimits, tool_calls_used: u8, query_retries_used: u8) -> Self {
        Self {
            tool_calls_used,
            tool_calls_limit: limits.max_tool_calls,
            query_retries_used,
            query_retries_limit: limits.max_query_retries,
            schema_repairs_used: 0,
            schema_repairs_limit: limits.max_schema_repairs,
            model_input_tokens: 0,
            model_output_tokens: 0,
        }
    }

    pub(crate) const fn with_model_usage(
        limits: OrchestratorLimits,
        tool_calls_used: u8,
        query_retries_used: u8,
        input_tokens: u32,
        output_tokens: u32,
        schema_repairs_used: u8,
    ) -> Self {
        let schema_repairs_used = if schema_repairs_used < limits.max_schema_repairs {
            schema_repairs_used
        } else {
            limits.max_schema_repairs
        };
        Self {
            schema_repairs_used,
            model_input_tokens: input_tokens,
            model_output_tokens: output_tokens,
            ..Self::rules_only(limits, tool_calls_used, query_retries_used)
        }
    }
}
