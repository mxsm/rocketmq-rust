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

use crate::authentication::enums::subject_type::SubjectType;

pub trait Subject {
    fn subject_key(&self) -> &str;

    fn subject_type(&self) -> SubjectType;

    #[inline]
    fn is_subject(&self, subject_type: SubjectType) -> bool {
        self.subject_type() == subject_type
    }
}
