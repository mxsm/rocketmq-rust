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

#[test]
fn request_header_codec_v2_reports_actionable_compile_errors() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/request_header_codec_v2/pass/*.rs");
    tests.compile_fail("tests/ui/request_header_codec_v2/fail/*.rs");
}
