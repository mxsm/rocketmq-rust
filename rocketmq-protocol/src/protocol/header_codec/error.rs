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

/// Adapts a classified codec error to the legacy remoting error boundary.
#[doc(hidden)]
#[cold]
#[inline(never)]
pub fn into_rocketmq_error(_error: ProtocolContractViolation) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_header_error("Request header is invalid")
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;

    use super::*;

    #[test]
    fn legacy_adapter_uses_the_catalog_owned_fixed_message() {
        const SENTINEL: &str = "header-validation-secret";
        let error = ProtocolContractViolation::Validation {
            header: "ExampleHeader",
            rule: SENTINEL,
        };

        let adapted = into_rocketmq_error(error);

        assert_eq!(adapted.kind(), ErrorKind::RequestHeaderError);
        assert_eq!(adapted.boundary_view().message(), "Request header is invalid");
        assert!(!adapted.to_string().contains(SENTINEL));
    }
}
