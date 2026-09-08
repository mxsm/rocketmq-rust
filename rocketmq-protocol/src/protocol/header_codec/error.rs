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

use crate::ProtocolContractViolation;

/// Adapts a classified codec error to the canonical error boundary.
#[doc(hidden)]
#[cold]
#[inline(never)]
pub fn into_error(_error: ProtocolContractViolation) -> rocketmq_error::Error {
    crate::error::request_header_failure()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_adapter_uses_the_catalog_owned_fixed_message() {
        const SENTINEL: &str = "header-validation-secret";
        let error = ProtocolContractViolation::Validation {
            header: "ExampleHeader",
            rule: SENTINEL,
        };

        let adapted = into_error(error);

        assert_eq!(adapted.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);
        assert_eq!(adapted.descriptor().public_message(), "Request header is invalid");
        assert!(!adapted.to_string().contains(SENTINEL));
    }
}
